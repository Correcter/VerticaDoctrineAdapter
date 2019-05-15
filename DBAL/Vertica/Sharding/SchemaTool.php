<?php

namespace VerticaDoctrineAdapter\DBAL\Vertica\Sharding;

use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\Tools\SchemaTool as BaseSchemaTool;


class SchemaTool extends BaseSchemaTool
{
    /**
     *
     * @var EntityManagerInterface
     */
    private $em;
    
    
    public function __construct(EntityManagerInterface $em)
    {
        parent::__construct($em);
        
        $this->em = $em;
    }

    

    public function updateSchema(array $classes, $saveMode = false, $prefix = null)
    {

        $shardManager = new PoolingShardManager($this->em->getConnection());
        
        $schema = $this->getSchemaFromMetadata($classes);
        
        return $shardManager->updateSchema($schema, $saveMode, $prefix);
    }
}