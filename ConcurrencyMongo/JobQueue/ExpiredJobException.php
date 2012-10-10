<?php

namespace ConcurrencyMongo\JobQueue;

use ConcurrencyMongo\JobQueue\JobQueueException;

class ExpiredJobException extends JobQueueException{

  public function __construct($message='expired job exception', $code = 0, Exception $previous = null) {
    parent::__construct($message, $code, $previous);
  }

}


