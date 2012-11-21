concurrency-mongodb
==================

MongoDBを使った並列処理のライブラリです.

素のPHP自体はマルチスレッド、マルチプロセスに対応していないので
多重起動、サーバのクラスタリングを組んだ場合に利用できるようになっています.



MongoDB上の構造
----

Documents
- JobQueue_${name}
 - _id
   object. ObjectID
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
- JobQueue_${name}.lockBy
y

まずロックしていないJobを

Project setup
----

    $ cd $PROJECT_HOME
    $ curl -s https://getcomposer.org/installer | php -- --install-dir=bin
    $ ./bin/composer.phar install

### Testing

    $ cd $PROJECT_HOME
    $ ./bin/phpunit

