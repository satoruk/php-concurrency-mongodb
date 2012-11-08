<?php

namespace ConcurrencyMongo\JobQueue;

use ConcurrencyMongo\JobQueue\Job;


interface JobWorker {

  /**
   * This JobWorker available for label
   *
   * @param string $label label
   * @return boolean true is available
   */
  public function isWorkable($label);

  /**
   * assign a job
   *
   * @param string $label label
   * @param object $job a job
   * @return void
   */
  public function assignJob($label, Job $job);
}


