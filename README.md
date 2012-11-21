concurrency-mongodb
==================

MongoDBを使った並列処理のライブラリです.

素のPHP自体はマルチスレッド、マルチプロセスに対応していないので
多重起動、サーバのクラスタリングを組んだ場合に利用できるようになっています.


JobQueue
----

ジョブキューイングのライブラリ群


ResourcePool
----

排他的なリソース管理


MongoDB上の構造
----


- JobQueue_${name}
 - _id           : object. ObjectID
 - value         : any types. this is a job value.
 - priority      : integer. 0 is most high priority. defult is 50
 - resolution    : 
 - opid          : Operation ID
 - lockExpiredAt : lock expired at
 - lockBy        : sets the locked uuid

ロック戦略
----

悲観的排他制御にてロックを行っています.

以下はロックの手順です.

利用するフィールド
- JobQueue_${name}.lockExpiredAt
 - ロック有効期限 MondoDB内の時間で判定
- JobQueue_${name}.lockBy
 - ロックをかけたuuid

lockExpiredAt以内に処理が終わらなかった場合、再処理されます.


Project setup
----

### For Developer

開発環境の構築手順

    $ git clone git://github.com/satoruk/php-concurrency-mongodb.git concurrency-mongodb
    $ cd concurrency-mongodb
    $ curl -s https://getcomposer.org/installer | php -- --install-dir=bin
    $ ./bin/composer.phar install --dev

#### Testing

    $ ./bin/phpunit

