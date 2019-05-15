<?php

namespace VerticaDoctrineAdapter\DBAL\Vertica\Schema;

use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Event\SchemaIndexDefinitionEventArgs;
use Doctrine\DBAL\Events;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\ForeignKeyConstraint;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\PostgreSqlSchemaManager;
use Doctrine\DBAL\Schema\Sequence;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Schema\SchemaConfig;
use Doctrine\DBAL\Types\Type;


/**
 * Description of VerticaSchemaManager
 *
 * @author Vitaly Dergunov (<correcter@inbox.ru>)
 */
class VerticaSchemaManager extends AbstractSchemaManager
{
    
    /**
     * @var array
     */
    private $existingSchemaPaths;
    
    /**
     * {@inheritdoc}
     */
    protected function getPortableNamespaceDefinition(array $namespace)
    {
        return $namespace['name'];
    }
    
    /**
     * Convert platform results for sequence definition to a portable format
     *
     * @param array $sequence
     *
     * @return Sequence
     *
     * @see AbstractSchemaManager::listSequences()
     * @see VerticaPlatform::getListSequencesSQL()
     */
    protected function _getPortableSequenceDefinition($sequence) : Sequence
    {
        return new Sequence($sequence['name'], $sequence['allocationSize'], $sequence['initialValue'], $sequence['cache']);
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableTableColumnDefinition($tableColumn): \Doctrine\DBAL\Schema\Column
    {
        // Remove size declaration from type, ex. numeric(10,2) -> numeric
        $dbType = rtrim($tableColumn['data_type'], '()0123456789,');
        if($dbType === 'varchar' && $tableColumn['character_maximum_length'] >= 65000){
            $dbType = 'text';
        }
        
        $type = $this->_platform->getDoctrineTypeMapping($dbType);
        $type = $this->extractDoctrineTypeFromComment($tableColumn['comment'], $type);
        $tableColumn['comment'] = $this->removeDoctrineTypeFromComment($tableColumn['comment'], $type);
        
        if(preg_match("/^'(.*)'(::.*)?$/", $tableColumn['column_default'], $matches)){
            $tableColumn['column_default'] = $matches[1];
        }
        if(stripos($tableColumn['column_default'], 'NULL') === 0){
            $tableColumn['column_default'] = null;
        }
        if(!empty($tableColumn['column_default']) && (string) $type == 'boolean'){
            $tableColumn['column_default'] = $tableColumn['column_default'] === 'true' ? true : false;
        }
        
        $options = [
            'length' => $tableColumn['character_maximum_length'],
            'notnull' => !$tableColumn['is_nullable'],
            'default' => $tableColumn['column_default'],
            'primary' => $tableColumn['constraint_type'] == 'p',
            'precision' => $tableColumn['numeric_precision'],
            'scale' => $tableColumn['numeric_scale'],
            'fixed' => $dbType == 'char' ? true : ($dbType == 'varchar' ? false : null),
            'unsigned' => false,
            'autoincrement' => (bool) $tableColumn['is_identity'],
            'comment' => $tableColumn['comment'],
            'customSchemaOptions' => [
                'encoding' => $tableColumn['encoding'] ?? 'AUTO'
            ]
        ];
        
        return new Column($tableColumn['column_name'], Type::getType($type), $options);
    }
    /**
     * Convert platform sql results for index definitions to a portable format
     *
     * @param array $tableIndexRows
     * @param string|Table|null $tableName
     *
     * @return Index[]
     *
     * @see AbstractSchemaManager::listTableIndexes()
     * @see VerticaPlatform::getListTableIndexesSQL()
     */
    protected function _getPortableTableIndexesList($tableIndexRows, $tableName = null)
    {        
        $result = [];
        foreach($tableIndexRows as $tableIndex){
            $indexName = $keyName = $tableIndex['constraint_name'];
            if($tableIndex['constraint_type'] == 'p'){
                $keyName = 'primary';
            }
            $keyName = strtolower($keyName);
            if(!isset($result[$keyName])){
                $result[$keyName] = [
                    'name' => $indexName,
                    'columns' => [$tableIndex['column_name']],
                    'unique' => true, // we have only primary and unique constraints,
                    'primary' => $tableIndex['constraint_type'] == 'p'
                ];
            }
            else{
                $result[$keyName]['columns'][] = $tableIndex['column_name'];
            }
        }

        $eventManager = $this->_platform->getEventManager();
        $indexes = [];
        foreach($result as $indexKey => $data){
            $index = null;
            $defaultPrevented = false;
            if(null !== $eventManager && $eventManager->hasListeners(Events::onSchemaIndexDefinition)){
                $eventArgs = new SchemaIndexDefinitionEventArgs($data, $tableName, $this->_conn);
                $eventManager->dispatchEvent(Events::onSchemaIndexDefinition, $eventArgs);
                $defaultPrevented = $eventArgs->isDefaultPrevented();
                $index = $eventArgs->getIndex();
            }
            if(!$defaultPrevented){
                $index = new Index($data['name'], $data['columns'], $data['unique'], $data['primary']);
            }
            if($index){
                $indexes[$indexKey] = $index;
            }
        }

        return $indexes;
    }
    
    /**
     * Lists the indexes for a given table returning an array of Index instances.
     *
     * Keys of the portable indexes list are all lower-cased.
     *
     * @param string $table The name of the table.
     *
     * @return \Doctrine\DBAL\Schema\Index[]
     */
    public function listTableIndexes($table)
    {
        $sql = $this->_platform->getListTableIndexesSQL($table, $this->_conn->getDatabase());

        $tableIndexes = $this->_conn->fetchAll($sql);

        return $this->_getPortableTableIndexesList($tableIndexes, $table);
    }
    /**
     * {@inheritDoc}
     */
    public function getSchemaSearchPaths()
    {
        $params = $this->_conn->getParams();
        
        $schema = explode(',', $this->_conn->fetchColumn('SHOW search_path', [], 1));
   
        if($this->_conn->getUsername()){
            $schema = str_replace('"$user"', $this->_conn->getUsername(), $schema);
        }

        return array_map('trim', $schema);
    }
    /**
     * Gets names of all existing schemas in the current users search path.
     *
     * This is a Vertica only function.
     *
     * @return array
     */
    public function getExistingSchemaSearchPaths()
    {
        if ($this->existingSchemaPaths === null) {
            $this->determineExistingSchemaSearchPaths();
        }

        return $this->existingSchemaPaths;
    }
    
    
    /**
     * Sets or resets the order of the existing schemas in the current search path of the user.
     *
     * This is a Vertica only function.
     *
     * @return void
     */
    public function determineExistingSchemaSearchPaths()
    {
        $names = $this->listNamespaceNames();
        $paths = $this->getSchemaSearchPaths();

        $this->existingSchemaPaths = array_filter($paths, function ($v) use ($names) {
            return in_array($v, $names);
        });
    }
    
    
    
    /**
     * {@inheritdoc}
     */
    protected function _getPortableTableDefinition($table)
    {
        $schemas = $this->getExistingSchemaSearchPaths();
        $firstSchema = array_shift($schemas);
        
        if ($table['schema'] == $firstSchema){
            return $table['name'];
        }
        else{
            return $table['schema'] . '.' . $table['name'];
        }
    }
    
    /**
     * Creates the configuration for this schema.
     *
     * @return \Doctrine\DBAL\Schema\SchemaConfig
     */
    public function createSchemaConfig()
    {
        $schemaConfig = new SchemaConfig();
        $schemaConfig->setMaxIdentifierLength($this->_platform->getMaxIdentifierLength());
        $schemaConfig->setName($this->_platform->getDefaultSchemaName());
        
        $params = $this->_conn->getParams();
        if (isset($params['defaultTableOptions'])){
            $schemaConfig->setDefaultTableOptions($params['defaultTableOptions']);
        }

        return $schemaConfig;
    }
    
}
