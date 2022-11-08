#### Library for interaction with DBMS [Vertica](https://www.vertica.com /) via [Doctrine](http://doctrine-project.org /)

##### VerticaDoctrineAdapter

*Installation and deployment*

``` composer install ```

- The installer must pull up all the necessary `vendor` packages.

#### Usage examples

- Create a shard manager

`$shardManager = new PoolingShardManager($em->getConnection());`

- Set the parameters of the **Client entity**

```
   $client = new \Entity\Client();
   
   /**
    *  @param \DateTime $lastLoadCampaigns
    */
   $client->setLastLoadCampaigns();
   
   /**
    *  @param Campaign $campaign
    */
    $client->addCampaign($campaign);
       
   /**
    * @param Segment $segment
    */
    $segment = new \ApiBundle\Entity\Segment();
    $client->addSegment($segment);
    ....
```

- Passing the Doctrine entity to ShardManager

`$shardManager->selectShardByEntity($client);`

- Getting an initialized entity with data:

```
print_r($client->getCampaigns())

```
