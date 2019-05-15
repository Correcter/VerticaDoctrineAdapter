<?php

namespace VerticaDoctrineAdapter\DBAL\Vertica\Driver;



/**
 * Description of VerticaException
 *
 * @author Vitaly Dergunov (<correcter@inbox.ru>)
 */
class VerticaException extends \RuntimeException
{  
    public static function fromConnection($dbh)
    {
        return new self(odbc_errormsg($dbh), (integer) odbc_error($dbh));
    }
}
