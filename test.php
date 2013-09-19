<?php
require_once 'uguisudani_client.php';

$cli = UguisudaniClient::connect('localhost');

$a = time(true);
 for ($i = 0; $i < 100000; $i++)
     $cli->put("A", array('count' => $i));
var_dump(microtime(true) - $a);

var_dump($cli->put("A", array('a' => 'b')));
var_dump($cli->put("A", array('a' => 'c')));
var_dump($cli->put("A", array('a' => 'd')));

$data = $cli->get("A", array('count' => 50000));
var_dump($data);
