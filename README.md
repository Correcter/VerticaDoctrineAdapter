#### Библиотека для взаимодействия с СУБД [Vertica](https://www.vertica.com/) через [Doctrine](http://doctrine-project.org/)

##### VerticaDoctrineAdapter

*Установка и развертывание*

``` composer install ```

- Установщик должен подтянуть все необходимые ``vendor`` - пакеты.

#### Примеры использования

- Создадим менеджер шардов

`$shardManager = new PoolingShardManager($em->getConnection());`

- Зададим параметры сущности **Client**

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

- Передадим сущность Doctrine в ShardManager

`$shardManager->selectShardByEntity($client);`

- Получаем инициализированную сущность с данными:

```
print_r($client->getCampaigns())

```