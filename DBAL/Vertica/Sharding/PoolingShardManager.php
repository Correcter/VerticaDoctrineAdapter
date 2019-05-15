<?php

namespace VerticaDoctrineAdapter\DBAL\Vertica\Sharding;

use Doctrine\DBAL\Schema\Comparator;

use Doctrine\DBAL\Schema\Schema;

/**
 * Class PoolingShardManager
 * @package VerticaDoctrineAdapter\DBAL\Vertica\Sharding
 */
class PoolingShardManager
{

    /**
     * @var \VerticaDoctrineAdapter\DBAL\Vertica\Sharding\PoolingShardConnection
     */
    private $conn;

    /**
     * 
     * @param \VerticaDoctrineAdapter\DBAL\Vertica\Sharding\PoolingShardConnection $conn
     */
    public function __construct(PoolingShardConnection $conn)
    {
        $this->conn = $conn;
    }

    /**
     * @param $shardId
     * @return string
     */
    public function getConnectionShardQuery($shardId)
    {
        $params = $this->conn->getParams();
        
        if(!isset($params['shards'][$shardId])){
            throw new \RuntimeException("Unknown shard id #{$shardId}");
        }
        
        return  "CONNECT TO VERTICA {$params['shards'][$shardId]['dbname']} USER {$params['shards'][$shardId]['user']}  PASSWORD '{$params['shards'][$shardId]['password']}' ON '{$params['shards'][$shardId]['host']}', {$params['shards'][$shardId]['port']};";
    }

    /**
     * @param $shardId
     * @return mixed
     */
    public function getDatabaseNameForShardId($shardId)
    {
        $params = $this->conn->getParams();
        
        if(!isset($params['shards'][$shardId])){
            throw new \RuntimeException("Unknown shard id #{$shardId}");
        }
        
        return $params['shards'][$shardId]['dbname'];
    }

    /**
     * @return array
     */
    public function getShards()
    {
        $params = $this->conn->getParams();
        $shards = [];

        foreach($params['shards'] as $shard){
            $shards[] = ['id' => $shard['id']];
        }

        return $shards;
    }

    /**
     * Функция выполняет запрос на всех шардах и на всех схемах (базах данных)
     * 
     * @param string $sql
     * @param array $params
     * @param array $types
     * @return array
     * @throws \RuntimeException
     */
    public function queryAll($sql, array $params, array $types)
    {
        $shards = $this->getShards();
        if(!$shards){
            throw new \RuntimeException("No shards found.");
        }

        $result = [];

        foreach($shards as $shard)
        {
            $this->conn->connect($shard['id']);
            
            $schemas = $this->conn->getSchemaManager()->getSchemaNames();
    
            foreach($schemas as $schema)
            {
                $this->conn->getSchemaManager()->setSchemaSearchPaths($schema);
                
                foreach($this->conn->fetchAll($sql, $params, $types) as $row)
                {
                    $result[] = $row;
                }
            }

        }

        return $result;
    }

    /**
     * Функция выполняет запрос на всех шардах и на всех схемах (базах данных)
     * 
     * @param string $sql
     * @param array $params
     * @param array $types
     * @return array
     * @throws \RuntimeException
     */
    public function executeAll($sql, array $params = [] , array $types = [])
    {
        $shards = $this->getShards();
        if(!$shards)
        {
            throw new \RuntimeException("No shards found.");
        }

        $result = [];

        foreach($shards as $shard)
        {
            $this->selectShardById($shard['id']);
            $this->conn->executeQuery($sql, $params, $types);
        }
        
        return $result;
    }

    /**
     * @param Schema $toSchema
     * @param bool $saveMode
     * @return mixed
     */
    public function getUpdateSchemaSql(Schema $toSchema, $saveMode = false)
    {
        $fromSchema = $this->conn->getSchemaManager()->createSchema();

        $comparator = new Comparator();

        $schemaDiff = $comparator->compare($fromSchema, $toSchema);

        if ($saveMode) {
            return $schemaDiff->toSaveSql($this->conn->getSchemaManager()->getDatabasePlatform());
        }
        return $schemaDiff->toSql($this->conn->getSchemaManager()->getDatabasePlatform());
    }

    /**
     * @param Schema $toSchema
     * @param bool $saveMode
     * @return mixed
     */
    public function updateCurrentSchema(Schema $toSchema, $saveMode = false)
    {
        $shards = $this->getShards();
        if(!$shards)
        {
            throw new \RuntimeException("No shards found.");
        }

        $updateSchemaSql = $this->getUpdateSchemaSql($toSchema, $saveMode);
        
        if($updateSchemaSql AND $saveMode)
        {
            foreach($updateSchemaSql as $sql)
                $this->conn->executeQuery($sql);
        }
        
        return $updateSchemaSql;
    }


    /**
     * @param Schema $toSchema
     * @param bool $saveMode
     * @param null $prefix
     * @return array
     */
    public function updateSchema(Schema $toSchema, $saveMode = false, $prefix = null)
    {
        
        $updateSchemaSqls = $this->getUpdateSchemaSql($toSchema, $saveMode); 
        
        if($saveMode){
            foreach($updateSchemaSqls as $sql){
                $this->conn->executeQuery($sql);
            }
        }

        $shards = $this->getShards();
        if(!$shards){
            throw new \RuntimeException("No shards found.");
        }

        $updateSchemaSql = [];

        foreach($shards as $shard){
            $this->selectShardById($shard['id']);

            foreach($this->getSchemaNames($prefix) as $schemaName)
            {
                $this->setSchema($schemaName);

                $updateSchemaSql[$schemaName] = $this->getUpdateSchemaSql($toSchema, $saveMode);
                if($updateSchemaSql[$schemaName] AND $saveMode)
                {
                    foreach($updateSchemaSql[$schemaName] as $sql)
                    {
                        if($sql) {
                            $this->conn->executeQuery($sql);
                        }
                    }

                }
            }

        }

        return $updateSchemaSql;
    }
    
    /**
     * Выбор шарда на сущности
     * 
     * @param EntityShardInterface $entity
     * @return true|false
     */
    public function selectShardByEntity(EntityShardInterface $entity)
    {
        return $this->conn->connect($entity->getVerticaShardId());
    }

    /**
     * Выбор шарда по его идентификатору
     * 
     * @param int $id
     * @return true|false
     */
    public function selectShardById($id)
    {
        return $this->conn->connect($id);
    }

    /**
     * Устанавливает схему для текущего соединения
     * 
     * @param string $schemaName
     * @return true|false
     */
    public function setSchema($schemaName)
    {
        return $this->conn->getSchemaManager()->setSchemaSearchPaths($schemaName);
    }
    
    
    /**
     * Возвращает все схемы в текущем соединении
     * @return array 
     */
    public function getSchemaNames($prefix =null)
    {
        return $this->conn->getSchemaManager()->getSchemaNames($prefix);
    }

    /**
     * @param $shardName
     * @param int $connId
     */
    public function selectShardByName($shardName, $connId = 1)
    {
        $this->conn->connect($connId);

        $this->conn->getSchemaManager()->setSchemaSearchPaths($shardName);

        $this->currentProject = $shardName;
    }

    /**
     * Функция выделяет идентификатор шарда
     * На данный момент, шард выделяется рандомно, в последствии моно переопределить 
     * 
     * @return integer
     * @throws \RuntimeException
     */
    public function getAllocateShardId()
    {
        $shards = $this->getShards();
        
        if(!$shards){
            throw new \RuntimeException("No shards found.");
        }
        
        $shard = $shards[array_rand($shards)];
        
        return $shard['id'];
        
    }
    
}
