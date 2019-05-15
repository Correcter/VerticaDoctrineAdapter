<?php

namespace VerticaDoctrineAdapter\DBAL\Vertica\Driver;

use Doctrine\DBAL\SQLParserUtils;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\Exception\SyntaxErrorException;

/**
 * Description of Statement
 *
 * @author Vitaly Dergunov (<correcter@inbox.ru>)
 */
class VerticaStatement implements \Iterator, Statement
{
    /**
     * @var odbc link resource
     */
    private $dbh;
    private $originalQuery;
    private $query;
    private $paramMap = [];
    private $params = [];
    private $executed = false;
    private $defaultFetchMode = \PDO::FETCH_BOTH;

    /**
     * 
     * @param resource $dbh odbc link resource
     * @param string $query
     */
    public function __construct($dbh, $query)
    {
        $this->dbh = $dbh;
        $this->originalQuery = $query;
        
        $this->parseQuery($query);
    }
    
    /**
     * @return string
     */
    public function getSql()
    {
        return $this->query;
    }

    /**
     * @param $query
     */
    private function parseQuery($query)
    {
        $this->query = $query;
        $this->paramMap = [];
        $this->params = [];
        
        $positions = array_flip(SQLParserUtils::getPlaceholderPositions($query));
        
        if($positions){
            if(SQLParserUtils::getPlaceholderPositions($query, false)){
                throw new SyntaxErrorException('Positional and named parameters can not be mixed');
            }
            
            $this->paramMap = array_combine(range(1, count($positions)), $positions);
            
            return;
        }
        
        $positions = SQLParserUtils::getPlaceholderPositions($query, false);
        
        if($positions){
            $queryLength = strlen($query);
            $queryParts = [$query];
            $i = 0;
            foreach($positions as $pos => $param){
                $this->paramMap[$param] = $i;
                $lastPart = array_pop($queryParts);
                $queryParts[] = substr($lastPart, 0, -1 * ($queryLength - $pos));
                $queryParts[] = substr($lastPart, $pos + strlen($param) + 1 );
                $i++;
            }
            $this->query = implode('?', $queryParts);
        }
        
        return;
    }
    
    
    /**
     * {@inheritDoc}
     */
    private function prepare()
    {
        $this->sth = @odbc_prepare($this->dbh, $this->query);
        
        if(!$this->sth){
            throw VerticaException::fromConnection($this->dbh);
        }
    }
    /**
     * {@inheritDoc}
     */
    public function execute($params = null)
    {
        $this->prepare();
        $this->executed = false;
        
        
        if($params){
            foreach($params as $pos => $value){
                if(is_int($pos)){
                    $pos += 1;
                }
                $this->bindValue($pos, $value);
            }
        }

        if(count($this->params) != count($this->paramMap)){
            throw new VerticaException(sprintf(
                    'Parameter count (%s) does not match prepared placeholder count (%s)', count($params), count($this->paramMap)
            ));
        }
        
        if(!@odbc_execute($this->sth, $this->params)){
            throw VerticaException::fromConnection($this->dbh);
        }
        
        $this->executed = true;
        return true;
    }
    /**
     * {@inheritDoc}
     */
    public function bindParam($column, &$variable, $type = null, $length = null)
    {
        if(!isset($this->paramMap[$column])){
            throw new VerticaException(
                sprintf('Parameter identifier "%s" is not presented in the query "%s"', $column, $this->originalQuery)
            );
        }
        $this->params[$this->paramMap[$column]] = &$variable;
        
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function bindValue($param, $value, $type = null)
    {
        return $this->bindParam($param, $value, $type);
    }

    /**
     * {@inheritDoc}
     */
    public function closeCursor()
    {
        return odbc_free_result($this->sth);
    }

    /**
     * {@inheritDoc}
     */
    public function columnCount()
    {
        return odbc_num_fields($this->sth);
    }

    /**
     * {@inheritDoc}
     */
    public function current()
    {
        return $this->current;
    }

    /**
     * {@inheritDoc}
     */
    public function errorCode()
    {
        return odbc_error($this->dbh);
    }

    /**
     * {@inheritDoc}
     */
    public function errorInfo()
    {
        return [
            'code' => odbc_error($this->dbh),
            'message' => odbc_errormsg($this->dbh)
        ];
    }

    /**
     * {@inheritDoc}
     */
    public function fetch($fetchMode = null, $cursorOrientation = \PDO::FETCH_ORI_NEXT, $cursorOffset = 0)
    {   
        if(!odbc_fetch_row($this->sth)){
            return false;
        }
        
        $numFields = odbc_num_fields($this->sth);
        $row = [];
        
        $fetchMode = $fetchMode ? : $this->defaultFetchMode;
        switch($fetchMode){
            case \PDO::FETCH_ASSOC: 
                for($i = 1; $i <= $numFields; $i++){
                    $value = odbc_result($this->sth, $i);
                    $row[odbc_field_name($this->sth, $i)] = $value === '' ? null: $value;
                }
                break;
            case \PDO::FETCH_NUM:
                for($i = 1; $i <= $numFields; $i++){
                    $value = odbc_result($this->sth, $i);
                    $row[] = $value === '' ? null: $value;
                }
                break;
            case \PDO::FETCH_BOTH;
                for($i = 1; $i <= $numFields; $i++){
                    $value = odbc_result($this->sth, $i);
                    $row[] = $value === ''? null: $value;
                    $row[odbc_field_name($this->sth, $i)] = $value === '' ? null: $value;
                }
                break;
            default:
                throw new \InvalidArgumentException(sprintf('Unsupported fetch mode "%s"', $fetchMode));
        }
        
        return $row;
    }

    /**
     * {@inheritDoc}
     */
    public function fetchAll($fetchMode = null, $fetchArgument = null, $ctorArgs = null): array
    {
        $rows = [];
        while($row = $this->fetch($fetchMode)){
            $rows[] = $row;
        }
        return $rows;
    }

    /**
     * {@inheritDoc}
     */
    public function fetchColumn($columnIndex = 0)
    {
        if(!odbc_fetch_row($this->sth)){
            return false;
        }
        
        return odbc_result($this->sth, $columnIndex + 1);
    }

    /**
     * {@inheritDoc}
     */
    public function key()
    {
        return $this->key >= 0 ? $this->key : null;
    }

    /**
     * {@inheritDoc}
     */
    public function next()
    {
        if(!$this->executed){
            $this->execute();
        }
        $this->key++;
        $this->started = true;
        $this->current = $this->fetch();
    }

    /**
     * {@inheritDoc}
     */
    public function rewind()
    {
        if($this->started){
            throw new VerticaException('Statement can not be rewound after iteration is started');
        }
        
        $this->next();
    }

    /**
     * {@inheritDoc}
     */
    public function rowCount()
    {
        return odbc_num_rows($this->sth);
    }

    /**
     * {@inheritDoc}
     */
    public function setFetchMode($fetchMode, $arg2 = null, $arg3 = null)
    {
        if ($arg2 !== null || $arg3 !== null) {
            throw new \InvalidArgumentException("Does not support 2nd/3rd argument to setFetchMode()");
        }
        
        $this->defaultFetchMode = $fetchMode;
        
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function valid()
    {
        return $this->current !== false;
    }
}
