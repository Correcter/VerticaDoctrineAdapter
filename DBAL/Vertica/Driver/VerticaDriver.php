<?php

namespace VerticaDoctrineAdapter\DBAL\Vertica\Driver;


use Doctrine\DBAL\DBALException;
use VerticaDoctrineAdapter\DBAL\Vertica\Platform\VerticaPlatform;
use VerticaDoctrineAdapter\DBAL\Vertica\Schema\VerticaSchemaManager;

require_once __DIR__.'/../Functions/fputtsv.php';


/**
 * A Doctrine DBAL driver for the Vertica PHP extensions.
 *
 * @author Vitaly Dergunov (<correcter@inbox.ru>)
 */
class VerticaDriver  implements \Doctrine\DBAL\Driver
{
    /**
     * @param array $params
     * @param null $username
     * @param null $password
     * @param array $driverOptions
     * @return \Doctrine\DBAL\Driver\Connection
     */
    public function connect(array $params, $username = null, $password = null, array $driverOptions = array()): \Doctrine\DBAL\Driver\Connection
    {
        try {
            $conn = new VericaConnection(
                $this->constructPdoDsn($params, $driverOptions),
                $username,
                $password
            );
            
        }
        catch (\Exception $e){
            throw DBALException::driverException($this, $e);
        }
        
        return $conn;
    }

    /**
     * @param array $params
     * @param array $driverOptions
     * @return mixed|string
     */
    private function constructPdoDsn(array $params, array $driverOptions = [])
    {
        if(!empty($params['dsn'])){
            $dsn = $params['dsn'];
        }
        else
        {
            $dsn = 'Driver=' . (!empty($params['driverOptions']['odbc_driver']) ? $params['driverOptions']['odbc_driver'] : 'vertica') . ';';
            if(isset($params['host'])){
                $dsn .= 'Servername=' . $params['host'] . ';';
            }
            if(isset($params['port'])){
                $dsn .= 'Port=' . $params['port'] . ';';
            }
            if(isset($params['dbname'])){
                $dsn .= 'Database=' . $params['dbname'] . ';';
            }
            if($driverOptions){
                $dsn .= ';' . implode(
                    ';',
                    array_map(function ($key, $value){
                        return $key . '=' . $value;
                    }, array_keys($driverOptions), $driverOptions)
                );

            }

        }
        return $dsn ;
    }

    /**
     * @param \Doctrine\DBAL\Connection $conn
     * @return mixed
     */
    public function getDatabase(\Doctrine\DBAL\Connection $conn)
    {
        $params = $conn->getParams();

        return $params['dbname'];
    }

    /**
     * @return \Doctrine\DBAL\Platforms\AbstractPlatform
     */
    public function getDatabasePlatform(): \Doctrine\DBAL\Platforms\AbstractPlatform
    {
        return new VerticaPlatform();
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return 'odbc_vertica';
    }

    /**
     * @param \Doctrine\DBAL\Connection $conn
     * @return \Doctrine\DBAL\Schema\AbstractSchemaManager
     */
    public function getSchemaManager(\Doctrine\DBAL\Connection $conn): \Doctrine\DBAL\Schema\AbstractSchemaManager
    {
        return new VerticaSchemaManager($conn);
    }
}
