<?php

namespace VerticaDoctrineAdapter\DBAL\Vertica\Driver;

use Doctrine\DBAL\ParameterType;

/**
 * Driver connections
 *
 * @author Vitaly Dergunov (<correcter@inbox.ru>)
 */
class VericaConnection implements \Doctrine\DBAL\Driver\Connection
{
    /**
     * Идентификатор соединения ODBC
     *
     * @var type 
     */
    private $dbh;

    /**
     * @param type $dsn
     * @param type $user
     * @param type $password
     * @throws VerticaException
     */
    public function __construct($dsn, $user, $password)
    {
        $isPersistant = (isset($params['persistent']) && $params['persistent'] == true);

        if($isPersistant) {
            $this->dbh = @odbc_pconnect($dsn, $user, $password);
        } 
        else{
            $this->dbh = @odbc_connect($dsn, $user, $password);
        }
        
        if(!$this->dbh){
            $error = error_get_last();
            throw new VerticaException($error['message']);
        }
    }

    /**
     * @param bool $flag
     */
    private function checkTransactionStarted($flag = true)
    {
        if($flag && !$this->inTransaction()){
            throw new VerticaException('Transaction was not started');
        }
        if(!$flag && $this->inTransaction()){
            throw new VerticaException('Transaction was already started');
        }
    }

    /**
     * @return bool
     */
    public function inTransaction()
    {
        return !odbc_autocommit($this->dbh);
    }

    /**
     * @return mixed
     */
    public function beginTransaction()
    {
        $this->checkTransactionStarted(false);
        return odbc_autocommit($this->dbh, false);
    }

    /**
     * @return bool
     */
    public function commit()
    {
        $this->checkTransactionStarted();
        return odbc_commit($this->dbh) && odbc_autocommit($this->dbh, true);
    }

    /**
     * @return bool
     */
    public function rollBack(): boolean
    {
        $this->checkTransactionStarted();
        return odbc_rollback($this->dbh) && odbc_autocommit($this->dbh, true);
    }

    /**
     * @return string
     */
    public function errorCode()
    {
        return odbc_error($this->dbh);
    }

    /**
     * @return array
     */
    public function errorInfo(): array
    {
        return [
            'code' => odbc_error($this->dbh),
            'message' => odbc_errormsg($this->dbh)
        ];
    }

    /**
     * @param $statement
     * @return mixed
     */
    public function exec($statement)
    {
        $stmt = $this->prepare($statement);
        $stmt->execute();
        return $stmt->rowCount();
    }

    /**
     * @param null $name
     * @return false|mixed
     */
    public function lastInsertId($name = null)
    {
        return $this->query("SELECT LAST_INSERT_ID();")->fetchColumn();
    }

    /**
     * @param $prepareString
     * @return \Doctrine\DBAL\Driver\Statement
     */
    public function prepare($prepareString): \Doctrine\DBAL\Driver\Statement
    {
        return new VerticaStatement($this->dbh, $prepareString);
    }

    /**
     * @return \Doctrine\DBAL\Driver\Statement
     */
    public function query(): \Doctrine\DBAL\Driver\Statement
    {
        $args = func_get_args();
        $sql = $args[0];
        $stmt = $this->prepare($sql);
        $stmt->execute();
        return $stmt;
    }

    /**
     * @param $input
     * @return string
     */
    public function quote($input, $type = ParameterType::STRING)
    {
        if(is_int($input) || is_float($input)){
            return $input;
        }
        
        return "'" . str_replace("'", "''", $input) . "'";
    }
}
