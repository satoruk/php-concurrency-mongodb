


MongoDB上の構造

Documents
- JobQueue_${name}
 Fields
 - _id           : object. ObjectID
 - value         : any types. this is a job value.
 - priority      : integer. 0 is most high priority. defult is 50
 - opid          : Operation ID
 - lockExpiredAt : lock expired at
 - lockBy        : sets the locked uuid

h2. ロック戦略

悲観的排他制御にてロックを行っています.
以下はロックの手順です.

利用するフィールド
- JobQueue_${name}.lockExpiredAt
- JobQueue_${name}.lockBy
y

まずロックしていないJobを




h1. SERP Crawler

h2. Project setup

$ cd $PROJECT_HOME
$ curl -s https://getcomposer.org/installer | php -- --install-dir=bin
$ ./bin/composer.phar install

h2. Testing

$ cd $PROJECT_HOME
$ phpunit

