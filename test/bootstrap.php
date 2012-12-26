<?php

error_reporting(E_ALL);

$loader = require __DIR__ . '/../vendor/autoload.php';
$loader->add('ConcurrencyMongo\Test', __DIR__);
$loader->register();

$file = 'log4php.local.xml';
if (!file_exists($file)) {
    $file = 'log4php.xml';
}
Logger::configure($file);
