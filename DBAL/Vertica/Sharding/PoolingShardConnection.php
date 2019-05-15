<?php

namespace VerticaDoctrineAdapter\DBAL\Vertica\Sharding;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Events;
use Doctrine\DBAL\Event\ConnectionEventArgs;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Configuration;
use Doctrine\Common\EventManager;

class PoolingShardConnection extends Connection
{

    /**
     * @var array
     */
    private $activeConnections;

    /**
     * @var integer
     */
    private $activeShardId;

    /**
     * @var array
     */
    private $connections;

    /**
     * @param array                         $params
     * @param \Doctrine\DBAL\Driver         $driver
     * @param \Doctrine\DBAL\Configuration  $config
     * @param \Doctrine\Common\EventManager $eventManager
     *
     * @throws \InvalidArgumentException
     */
    public function __construct(array $params, Driver $driver, Configuration $config = null, EventManager $eventManager = null)
    {

        if(!isset($params['shards'])){
            throw new \InvalidArgumentException("Connection Parameters require 'shards' configurations.");
        }
        
        foreach($params['shards'] as $shard){
            if(!isset($shard['id'])){
                throw new \InvalidArgumentException("Missing 'id' for one configured shard. Please specify a unique shard-id.");
            }

            if(!is_numeric($shard['id']) || $shard['id'] < 1){
                throw new \InvalidArgumentException("Shard Id has to be a non-negative number.");
            }

            if(isset($this->connections[$shard['id']])){
                throw new \InvalidArgumentException("Shard " . $shard['id'] . " is duplicated in the configuration.");
            }

            $this->connections[$shard['id']] = array_merge($params['global'], $shard);

        }
        
        $params['shards'] = $this->connections;
        
        parent::__construct($params, $driver, $config, $eventManager);
    }

    /**
     * Connects to a given shard.
     *
     * @param mixed $shardId
     *
     * @return boolean
     *
     * @throws \Doctrine\DBAL\Sharding\ShardingException
     */
    public function connect($shardId = null)
    {
        if($shardId === null && $this->_conn){
            return false;
        }

        if($shardId !== null && $shardId === $this->activeShardId){
            return false;
        }

        if($this->getTransactionNestingLevel() > 0){
            throw new ShardingException("Cannot switch shard when transaction is active.");
        }

        $this->activeShardId = (int) $shardId;

        if(isset($this->activeConnections[$this->activeShardId])){
            $this->_conn = $this->activeConnections[$this->activeShardId];
            return false;
        }

        $this->_conn = $this->activeConnections[$this->activeShardId] = $this->connectTo($this->activeShardId);

        if($this->_eventManager->hasListeners(Events::postConnect)){
            $eventArgs = new ConnectionEventArgs($this);
            $this->_eventManager->dispatchEvent(Events::postConnect, $eventArgs);
        }

        return true;
    }

    /**
     * Connects to a specific connection.
     *
     * @param string $shardId
     *
     * @return \Doctrine\DBAL\Driver\Connection
     */
    protected function connectTo($shardId)
    {
        $params = $this->getParams();

        $driverOptions = isset($params['driverOptions']) ? $params['driverOptions'] : [];

        $connectionParams = $this->connections[$shardId];

        $user = isset($connectionParams['user']) ? $connectionParams['user'] : null;
        $password = isset($connectionParams['password']) ? $connectionParams['password'] : null;

        return $this->_driver->connect($connectionParams, $user, $password, $driverOptions);
    }
    
    /**
     * Gets the hostname of the currently connected database.
     *
     * @return string|null
     */
    public function getHost()
    {
        $connectionParams = $this->connections[$this->activeShardId];
        return isset($connectionParams['host']) ? $connectionParams['host'] : null;
    }

    /**
     * Gets the port of the currently connected database.
     *
     * @return mixed
     */
    public function getPort()
    {
        $connectionParams = $this->connections[$this->activeShardId];
        return isset($connectionParams['port']) ? $connectionParams['port'] : null;
    }

    /**
     * Gets the username used by this connection.
     *
     * @return string|null
     */
    public function getUsername()
    {
        $connectionParams = $this->connections[$this->activeShardId];
        return isset($connectionParams['user']) ? $connectionParams['user'] : null;
    }

    /**
     * Gets the password used by this connection.
     *
     * @return string|null
     */
    public function getPassword()
    {
        $connectionParams = $this->connections[$this->activeShardId];
        return isset($connectionParams['password']) ? $connectionParams['password'] : null;
    }
    
    
    /**
     * Gets the name of the database this Connection is connected to.
     *
     * @return string
     */
    public function getDatabase()
    {
        $connectionParams = $this->connections[$this->activeShardId];
        return $connectionParams['dbname'] ?? null;
    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    /**
     * @param string|null $shardId
     *
     * @return boolean
     */
    public function isConnected($shardId = null)
    {
        if($shardId === null){
            return $this->_conn !== null;
        }

        return isset($this->activeConnections[$shardId]);
    }

    /**
     * @return void
     */
    public function close()
    {
        $this->_conn = null;
        $this->activeConnections = null;
    }

}
