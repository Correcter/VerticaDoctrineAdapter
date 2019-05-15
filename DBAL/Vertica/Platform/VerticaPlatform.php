<?php

namespace VerticaDoctrineAdapter\DBAL\Vertica\Platform;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\ColumnDiff;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\Sequence;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Schema\TableDiff;
use Doctrine\DBAL\Events;
use Doctrine\DBAL\Event\SchemaCreateTableEventArgs;
use Doctrine\DBAL\Event\SchemaCreateTableColumnEventArgs;

/**
 * Description of VerticaPlatform
 *
 * @author Vitaly Dergunov (<correcter@inbox.ru>)
 */
class VerticaPlatform extends AbstractPlatform
{

    /**
     * {@inheritDoc}
     */
    public function supportsSequences()
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsSchemas()
    {
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function getDefaultSchemaName()
    {
        return 'public';
    }

    /**
     * {@inheritDoc}
     */
    public function supportsIdentityColumns()
    {
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function supportsPartialIndexes()
    {
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function usesSequenceEmulatedIdentityColumns()
    {
        return true;
    }

    /**
     * @param string $tableName
     * @param string $columnName
     * @return string
     */
    public function getIdentitySequenceName($tableName, $columnName)
    {
        return $tableName . '_' . $columnName . '_seq';
    }

    /**
     * {@inheritDoc}
     */
    public function supportsCommentOnStatement()
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function prefersSequences(): bool
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function hasNativeGuidType(): bool
    {
        return true;
    }

    /**
     * @return string
     */
    public function getListNamespacesSQL(): string
    {
        return "SELECT table_schema AS name FROM v_catalog.tables GROUP BY name;";
    }

    /**
     * @param string $database
     * @return string
     */
    public function getListSequencesSQL($database): string
    {
        return "SELECT sequence_schema || '.' || sequence_name AS name,   increment_by AS allocationSize, minimum AS initialValue, session_cache_count AS cache FROM v_catalog.sequences";
    }

    /**
     * @return string
     */
    public function getListTablesSQL(): string
    {
        return "SELECT table_schema AS schema, table_name AS name FROM v_catalog.tables";
    }

    /**
     * Разбивает название таблицы на схему и название
     * @param string|null $name
     * @return array
     */
    private function splitTableName(string $name = null): array
    {
        $splited = [
            'schema' => 'public',
            'name' => $name,
        ];
        
        if(strpos($name, '.') !== false){
            $parts = explode('.', $name);
            $splited['schema'] = $parts[0];
            $splited['name'] = $parts[1];
        }
        
        return $splited;
    }

    /**
     * @param string|null $table
     * @param null $database
     * @return string
     */
    public function getListTableColumnsSQL($table = null, $database = null): string
    {
        $tableInfo = $this->splitTableName($table);
        
        return "SELECT 
                    col.column_name, 
                    col.data_type, 
                    col.character_maximum_length, 
                    col.numeric_precision, 
                    col.numeric_scale,
                    col.is_nullable, 
                    col.column_default, 
                    col.is_identity, 
                    con.constraint_type, 
                    com.comment,
                    IFNULL(pc.encoding_type, 'AUTO') AS encoding
                FROM v_catalog.tables t
                JOIN v_catalog.columns col ON t.table_id = col.table_id
                LEFT JOIN v_catalog.constraint_columns con ON con.table_id = col.table_id AND con.column_name = col.column_name AND constraint_type = 'p'
                LEFT JOIN v_catalog.comments com ON com.object_type = 'TABLE' AND com.object_name = col.table_name
                LEFT JOIN v_catalog.projections p ON  p.anchor_table_id = t.table_id
                LEFT JOIN v_catalog.projection_columns pc ON p.projection_id = pc.projection_id AND pc.table_column_id = col.column_id
                WHERE 
                t.table_schema = '{$tableInfo['schema']}' AND
                t.table_name = '{$tableInfo['name']}'  AND
                (p.is_super_projection = true OR p.is_super_projection IS NULL)";
    }

    /**
     * @return VerticaSQLKeywords
     */
    protected function getReservedKeywordsClass(): VerticaSQLKeywords
    {
        return VerticaSQLKeywords::class;
    }

    /**
     * @param string $table
     * @param null $database
     * @return string
     */
    public function getListTableForeignKeysSQL($table, $database = null)
    {
        $tableInfo = $this->splitTableName($table);
        
        return "SELECT 
                        constraint_id, 
                        constraint_name, 
                        column_name, 
                        reference_table_name, 
                        reference_column_name
                FROM 
                    v_catalog.foreign_keys
                WHERE 
                    table_schema = '{$tableInfo['schema']}' AND
                    table_name = '{$tableInfo['name']}'
                ";
    }
    
    /**
     * {@inheritDoc}
     */
    public function getListTableConstraintsSQL($table = null)
    {
        $tableInfo = $this->splitTableName($table);
        
        return "SELECT 
                        c.constraint_id, 
                        c.column_name, 
                        c.constraint_name, 
                        c.constraint_type 
                FROM 
                        v_catalog.constraint_columns c
                LEFT JOIN primary_keys p ON 
                                            p.constraint_id = c.constraint_id AND 
                                            p.column_name = c.column_name
                WHERE 
                        c.constraint_type IN ('u', 'p') AND 
                        c.table_schema = '{$tableInfo['schema']}' AND
                        c.table_name = '{$tableInfo['name']}'
                ORDER BY 
                        c.constraint_id, p.ordinal_position, c.column_name";
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableIndexesSQL($table, $currentDatabase = null)
    {
        // There is no indexes in Vertica but doctrine treats unique constraints as indexes
        return $this->getListTableConstraintsSQL($table);
    }

    /**
     * @param array $columnDef
     * @return string
     */
    protected function _getCommonIntegerTypeDeclarationSQL(array $columnDef): string
    {
        throw new \RuntimeException(__CLASS__ . ':' .  __FUNCTION__);
    }

    protected function initializeDoctrineTypeMappings()
    {
        $this->doctrineTypeMapping = [
            // Vertica has only 64-bit integer, but we will treat al ass integer except bigint
            'bigint' => 'integer',
            'integer' => 'integer',
            'int' => 'integer',
            'int8' => 'integer',
            'smallint' => 'integer',
            'tinyint' => 'integer',
            'boolean' => 'boolean',
            'varchar' => 'string',
            'character varying' => 'string',
            'char' => 'string',
            'character' => 'string',
            // custom type, Vertica has only varchar, but we will treat bi varchars (4k+) as text
            'text' => 'text',
            'date' => 'date',
            'datetime' => 'datetime',
            'smalldatetime' => 'datetime',
            'timestamp' => 'datetime',
            'timestamptz' => 'datetimetz',
            'time' => 'time',
            'timetz' => 'time',
            'float' => 'float',
            'float8' => 'float',
            'double precision' => 'float',
            'real' => 'float',
            'decimal' => 'decimal',
            'money' => 'decimal',
            'numeric' => 'decimal',
            'number' => 'decimal',
            'binary' => 'blob',
            'varbinary' => 'blob',
            'bytea' => 'blob',
            'raw' => 'blob'
        ];
    }

    /**
     * @param array $columnDef
     *
     * @return string
     */
    public function getBigIntTypeDeclarationSQL(array $columnDef): string
    {
        throw new \RuntimeException(__CLASS__ . ':' .  __FUNCTION__);
    }

    /**
     * @param array $field
     * @return string
     */
    public function getBlobTypeDeclarationSQL(array $field): string
    {
        throw new \RuntimeException(__CLASS__ . ':' .  __FUNCTION__);
    }

    /**
     * @param array $columnDef
     * @return string
     */
    public function getBooleanTypeDeclarationSQL(array $columnDef): string
    {
        throw new \RuntimeException(__CLASS__ . ':' .  __FUNCTION__);
    }

    /**
     * Gets the maximum length of a varchar field.
     *
     * @return integer
     */
    public function getVarcharMaxLength(): int
    {
        return 65000;
    }

    /**
     * @param int $length
     * @param bool $fixed
     * @return string
     */
    protected function getVarcharTypeDeclarationSQLSnippet($length, $fixed): string
    {
        return $fixed ? ($length ? 'CHAR(' . $length . ')' : 'CHAR(255)')
                : ($length ? 'VARCHAR(' . $length . ')' : 'VARCHAR(255)');
    }

    /**
     * @param array $field
     * @return string
     */
    public function getClobTypeDeclarationSQL(array $field = []): string
    {
        return 'LONG VARCHAR(' . $field['length'] . ')';
    }

    /**
     * @param array $columnDef
     * @return string
     */
    public function getIntegerTypeDeclarationSQL(array $columnDef = []): string
    {
        if(!empty($columnDef['autoincrement'])){
            return 'AUTO_INCREMENT';
        }
        
        return 'INTEGER';
    }

    /**
     * @param array $fieldDeclaration
     * @return string
     */
    public function getDateTypeDeclarationSQL(array $fieldDeclaration = []): string
    {
        return 'DATE';
    }

    /**
     * @param array $columnDef
     * @return string
     */
    public function getSmallIntTypeDeclarationSQL(array $columnDef = [])
    {
        return $this->getIntegerTypeDeclarationSQL($columnDef);
    }
    
    /**
     * {@inheritDoc}
     */
    public function getFloatDeclarationSQL(array $fieldDeclaration): string
    {
        return 'FLOAT';
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return 'vertica';
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateSequenceSQL(Sequence $sequence)
    {
        return 'CREATE SEQUENCE ' . $sequence->getQuotedName($this) .
               ' INCREMENT BY ' . $sequence->getAllocationSize() .
               ' MINVALUE ' . $sequence->getInitialValue() .
               ' START ' . $sequence->getInitialValue() .
               $this->getSequenceCacheSQL($sequence);
    }
    
    /**
     * Cache definition for sequences
     *
     * @param Sequence $sequence
     *
     * @return string
     */
    private function getSequenceCacheSQL(Sequence $sequence)
    {
        if($sequence->getCache() > 1){
            return ' CACHE ' . $sequence->getCache();
        }

        return '';
    }
    
    /**
     * Returns the SQL statement(s) to create a table with the specified name, columns and constraints
     * on this platform.
     *
     * @param \Doctrine\DBAL\Schema\Table   $table
     * @param integer                       $createFlags
     *
     * @return array The sequence of SQL statements.
     *
     * @throws \Doctrine\DBAL\DBALException
     * @throws \InvalidArgumentException
     */
    public function getCreateTableSQL(Table $table, $createFlags = AbstractPlatform::CREATE_INDEXES)
    {
        if(!is_int($createFlags)){
            throw new \InvalidArgumentException("Second argument of VerticaPlatform::getCreateTableSQL() has to be integer.");
        }

        if (count($table->getColumns()) === 0) {
            throw DBALException::noColumnsSpecifiedForTable($table->getName());
        }
        
        $tableName = $table->getQuotedName($this);
        $options = $table->getOptions();
        $options['uniqueConstraints'] = array();
        $options['indexes'] = array();
        $options['primary'] = array();

        if(($createFlags & self::CREATE_INDEXES) > 0){
            foreach ($table->getIndexes() as $index) {
                /* @var $index Index */
                if ($index->isPrimary()) {
                    $options['primary']       = $index->getQuotedColumns($this);
                    $options['primary_index'] = $index;
                } else {
                    $options['indexes'][$index->getQuotedName($this)] = $index;
                }
            }
        }

        $columnSql = array();
        $columns = array();

        foreach ($table->getColumns() as $column) {
            /* @var \Doctrine\DBAL\Schema\Column $column */

            if (null !== $this->_eventManager && $this->_eventManager->hasListeners(Events::onSchemaCreateTableColumn)) {
                $eventArgs = new SchemaCreateTableColumnEventArgs($column, $table, $this);
                $this->_eventManager->dispatchEvent(Events::onSchemaCreateTableColumn, $eventArgs);

                $columnSql = array_merge($columnSql, $eventArgs->getSql());

                if ($eventArgs->isDefaultPrevented()) {
                    continue;
                }
            }

            $columnData = $column->toArray();
            $columnData['name'] = $column->getQuotedName($this);
            $columnData['version'] = $column->hasPlatformOption("version") ? $column->getPlatformOption('version') : false;
            $columnData['comment'] = $this->getColumnComment($column);

            if (strtolower($columnData['type']) == "string" && $columnData['length'] === null) {
                $columnData['length'] = 255;
            }

            if (in_array($column->getName(), $options['primary'])) {
                $columnData['primary'] = true;
            }

            $columns[$columnData['name']] = $columnData;
        }

        if (($createFlags&self::CREATE_FOREIGNKEYS) > 0) {
            $options['foreignKeys'] = array();
            foreach ($table->getForeignKeys() as $fkConstraint) {
                $options['foreignKeys'][] = $fkConstraint;
            }
        }

        if (null !== $this->_eventManager && $this->_eventManager->hasListeners(Events::onSchemaCreateTable)) {
            $eventArgs = new SchemaCreateTableEventArgs($table, $columns, $options, $this);
            $this->_eventManager->dispatchEvent(Events::onSchemaCreateTable, $eventArgs);

            if ($eventArgs->isDefaultPrevented()) {
                return array_merge($eventArgs->getSql(), $columnSql);
            }
        }

        $sql = $this->_getCreateTableSQL($tableName, $columns, $options);
        
        if ($this->supportsCommentOnStatement()) {
            foreach ($table->getColumns() as $column) {
                $comment = $this->getColumnComment($column);

                if (null !== $comment && '' !== $comment) {
                    $sql[] = $this->getCommentOnColumnSQL($tableName, $column->getQuotedName($this), $comment);
                }
            }
        }

        return array_merge($sql, $columnSql);
    }

    /**
     * Obtains DBMS specific SQL code portion needed to declare a generic type
     * field to be used in statements like CREATE TABLE.
     *
     * @param string $name  The name the field to be declared.
     * @param array  $field An associative array with the name of the properties
     *                      of the field being declared as array indexes. Currently, the types
     *                      of supported field properties are as follows:
     *
     *      length
     *          Integer value that determines the maximum length of the text
     *          field. If this argument is missing the field should be
     *          declared to have the longest length allowed by the DBMS.
     *
     *      default
     *          Text value to be used as default for this field.
     *
     *      notnull
     *          Boolean flag that indicates whether this field is constrained
     *          to not be set to null.
     *      charset
     *          Text value with the default CHARACTER SET for this field.
     *      collation
     *          Text value with the default COLLATION for this field.
     *      unique
     *          unique constraint
     *      check
     *          column check constraint
     *      columnDefinition
     *          a string that defines the complete column
     *
     * @return string DBMS specific SQL code portion that should be used to declare the column.
     */
    public function getColumnDeclarationSQL($name, array $field)
    {
        if (isset($field['columnDefinition'])) {
            $columnDef = $this->getCustomTypeDeclarationSQL($field);
        } else {
            $default = $this->getDefaultValueDeclarationSQL($field);
            $notnull = (isset($field['notnull']) && $field['notnull']) ? ' NOT NULL' : '';
            $unique  = (isset($field['unique']) && $field['unique']) ? ' ' . $this->getUniqueFieldDeclarationSQL() : '';
            $check   = (isset($field['check']) && $field['check']) ? ' ' . $field['check'] : '';
            $typeDecl = $field['type']->getSqlDeclaration($field, $this);
            $encoding   = (isset($field['encoding']) && $field['encoding']) ? ' ENCODING ' . $field['encoding'] : '';
            $columnDef = $typeDecl . $default . $notnull . $unique . $check . $encoding ;
            
            if($this->supportsInlineColumnComments() && isset($field['comment']) && $field['comment'] !== ''){
                $columnDef .= " COMMENT " . $this->quoteStringLiteral($field['comment']);
            }
        }
        
        return $name . ' ' . $columnDef;
    }

    /**
     * {@inheritDoc}
     */
    protected function _getCreateTableSQL($tableName, array $columns, array $options = [])
    {
        $queryFields = $this->getColumnDeclarationListSQL($columns);
        
        if(isset($options['primary']) && !empty($options['primary'])){
            $keyColumns = array_unique(array_values($options['primary']));
            $queryFields .= ', PRIMARY KEY(' . implode(', ', $keyColumns) . ')';
        }
        
        $partition = (isset($options['partition']) && $options['partition']) ? ' PARTITION BY ' . $options['partition'] : '';
        
        $query = 'CREATE TABLE ' . $tableName . ' (' . $queryFields . ')' . $partition;
        $sql[] = $query;
        if(isset($options['foreignKeys'])){
            foreach((array) $options['foreignKeys'] as $definition){
                $sql[] = $this->getCreateForeignKeySQL($definition, $tableName);
            }
        }
        return $sql;
    }
    
    /**
     * {@inheritDoc}
     */
    public function getAlterTableSQL(TableDiff $diff)
    {
        $sql = [];
        $commentsSQL = [];
        $columnSql = [];
        
        foreach($diff->addedColumns as $column){
            if($this->onSchemaAlterTableAddColumn($column, $diff, $columnSql)) {
                continue;
            }
            
            $query = 'ADD ' . $this->getColumnDeclarationSQL($column->getQuotedName($this), $column->toArray());
            $sql[] = 'ALTER TABLE ' . $diff->getName($this)->getQuotedName($this) . ' ' . $query;

            $comment = $this->getColumnComment($column);

            if(null !== $comment && '' !== $comment){
                $commentsSQL[] = $this->getCommentOnColumnSQL(
                    $diff->getName($this)->getQuotedName($this),
                    $column->getQuotedName($this),
                    $comment
                );
            }
        }
        
        foreach($diff->removedColumns as $column){
            if($this->onSchemaAlterTableRemoveColumn($column, $diff, $columnSql)) {
                continue;
            }
            if($column->getDefault()){
                $sql[] = 'ALTER TABLE ' . $diff->getName($this)->getQuotedName($this) . ' ALTER COLUMN ' . $column->getQuotedName($this) . ' DROP DEFAULT';
            }
            $sql[] = 'SELECT MAKE_AHM_NOW();';
            $query = 'DROP COLUMN ' . $column->getQuotedName($this) . ' CASCADE';
            $sql[] = 'ALTER TABLE ' . $diff->getName($this)->getQuotedName($this) . ' ' . $query;
        }
        
        foreach($diff->changedColumns as $columnDiff){
            if($this->onSchemaAlterTableChangeColumn($columnDiff, $diff, $columnSql)){
                continue;
            }
            
            $oldColumnName = $columnDiff->getOldColumnName()->getQuotedName($this);
            $column = $columnDiff->column;

            if($columnDiff->hasChanged('default')){
                $defaultClause = null === $column->getDefault()
                    ? ' DROP DEFAULT'
                    : ' SET' . $this->getDefaultValueDeclarationSQL($column->toArray());
                $query = 'ALTER COLUMN ' . $oldColumnName . $defaultClause;
                $sql[] = 'ALTER TABLE ' . $diff->name . ' ' . $query;
            }
            
            if($columnDiff->hasChanged('type') || $columnDiff->hasChanged('precision') || $columnDiff->hasChanged('scale')){
                $type = $column->getType();

                $query = 'ALTER COLUMN ' . $oldColumnName . ' SET DATA TYPE ' . $type->getSqlDeclaration($column->toArray(), $this);
                $sql[] = 'ALTER TABLE ' . $diff->name . ' ' . $query;
            }
            
            if($columnDiff->hasChanged('notnull')){
                $query = 'ALTER COLUMN ' . $oldColumnName . ' ' . ($column->getNotNull() ? 'SET' : 'DROP') . ' NOT NULL';
                $sql[] = 'ALTER TABLE ' . $diff->name . ' ' . $query;
            }
            
            if($columnDiff->hasChanged('autoincrement')){
                // add autoincrement
                if($column->getAutoincrement()){
                    $seqName = $diff->name . '_' . $oldColumnName . '_seq';
                    
                    $sql[] = 'CREATE SEQUENCE ' . $seqName . '  INCREMENT BY 1 START WITH 1';
                    $sql[] = "SELECT setval('" . $seqName . "', (SELECT MAX(" . $oldColumnName . ") FROM " . $diff->name . "))";

                    $query = 'ALTER COLUMN ' . $oldColumnName . " SET DEFAULT nextval('" . $seqName . "')";
                    $sql[] = 'ALTER TABLE ' . $diff->name . ' ' . $query;
                }
                // Drop autoincrement
                else{
                    $query = 'ALTER COLUMN ' . $oldColumnName . ' DROP DEFAULT';
                    $sql[] = 'ALTER TABLE ' . $diff->name . ' ' . $query;
                }
            }
            
        
            if($columnDiff->hasChanged('comment')){
                $commentsSQL[] = $this->getCommentOnColumnSQL(
                    $diff->name,
                    $column->getName(),
                    $this->getColumnComment($column)
                );
            }
            
            if($columnDiff->hasChanged('length')){
                $query = 'ALTER COLUMN ' . $column->getName() . ' SET DATA TYPE ' . $column->getType()->getSqlDeclaration($column->toArray(), $this);
                $sql[] = 'ALTER TABLE ' .$diff->name . ' ' . $query;
            }
        }
     
        foreach($diff->renamedColumns as $oldColumnName => $column){
            if ($this->onSchemaAlterTableRenameColumn($oldColumnName, $column, $diff, $columnSql)) {
                continue;
            }

            $sql[] = 'ALTER TABLE ' . $diff->name . ' RENAME COLUMN ' . $oldColumnName . ' TO ' . $column->getQuotedName($this);
        }
        
        $tableSql = array();

        if(!$this->onSchemaAlterTable($diff, $tableSql)){
            if ($diff->newName !== false) {
                $sql[] = 'ALTER TABLE ' . $diff->name . ' RENAME TO ' . $diff->newName;
            }

            $sql = array_merge(
                $this->getPreAlterTableIndexForeignKeySQL($diff),
                $sql,
                $this->getPostAlterTableIndexForeignKeySQL($diff)
            );
        }
        
        $sql = array_merge($sql, $tableSql, $columnSql);
        
        $columnComments = [];
        /** @var ColumnDiff $columnDiff */
        foreach($diff->changedColumns as $columnDiff){
            if($columnDiff->hasChanged('comment') && $comment = $this->getColumnComment($columnDiff->column)){
                $columnComments[$columnDiff->column->getName()] = $comment;
            }
        }
        if(!empty($columnComments)){
            $sql[] = $this->getCommentOnTableColumnsSQL($diff->name, $columnComments);
        }
        
        return $sql;
    }

    /**
     * @param Index $index
     * @param Table|string $table
     * @return null|string
     */
    public function getCreateIndexSQL(Index $index, $table): ?string
    {
        if($index->isPrimary()){
            return $this->getCreatePrimaryKeySQL($index, $table);
        }
        if($index->isSimpleIndex()){
            trigger_error(
                sprintf(
                        'Can not create index "%s" for table "%s": %s does not support common indexes. Columns: %s',
                        $index->getName(),
                        $table,
                        $this->getName(),
                        implode(', ', $index->getColumns())
                    ), E_USER_NOTICE
            );
            return null;
        }
        return $this->getCreateConstraintSQL($index, $table);
    }


    /**
     * @return string
     */
    public function getPartitionsAuditSQL(): string
    {
        return ("
            SELECT
              partition_name as partition_name,
              table_name as table_name,
              (SUM(rows_count) - SUM(deleted_rows_count)) as rows_count,
              SUM(compressed_size_bytes) as compressed_size_bytes
            FROM
              (
                SELECT
                  partition_key as partition_name,
                  anchor_table_name as table_name,
                  MAX(ros_row_count) as rows_count,
                  MAX(ros_size_bytes) as compressed_size_bytes,
                  SUM(deleted_row_count) as deleted_rows_count
                FROM partitions
                FULL JOIN projections ON partitions.projection_id = projections.projection_id
                WHERE partition_key IS NOT NULL
                GROUP BY partition_key, anchor_table_name, ros_id, partitions.projection_id
              ) as summary_data
            WHERE partition_name IS NOT NULL
            GROUP BY partition_name, table_name
        ");
    }

    /**
     * @return string
     */
    public function getCompressionRatioSQL(): string
    {
        return ("
            SELECT
              database_size_bytes/(SELECT SUM(used_bytes) FROM projection_storage) as compression_ratio
            FROM
              license_audits
            WHERE
              audited_data = 'Total'
            ORDER BY audit_start_timestamp DESC
            LIMIT 1
        ");
    }
}
