<?php

namespace VerticaDoctrineAdapter\DBAL\Vertica\Sharding;

/**
 * @author Vitaly Dergunov
 */
interface EntityShardInterface
{
    public function setVerticaShardId($verticaShardId);
    public function getVerticaShardId();
}
