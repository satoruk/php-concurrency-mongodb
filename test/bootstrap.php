<?php

require_once('vendor/autoload.php');

$file = 'log4php.local.xml';
if(!file_exists($file)){
  $file = 'log4php.xml';
}
Logger::configure($file);


