<?php
class IOError extends Exception {
    public $errno;
    public $errstr;

    function __construct($errno = null, $errstr = null) {
        $this->errno = $errno;
        $this->errstr = $errstr;
    }
}

class Journal {
    public $file;
    protected $f;

    public function __construct($file) {
        $this->file = $file;
        $this->f = fopen($file, 'a');
        fseek($this->f, 0, SEEK_END);
    }

    public function getPosition() {
        return ftell($this->f);
    }

    public function insert($bucket, &$datum, $now) {
        fwrite($this->f, serialize(array('insert', $now, $bucket->name, $datum)) . "\n");
    }

    public function remove($bucket, $id, $now) {
        fwrite($this->f, serialize(array('remove', $now, $this->bucket, $id)) . "\n");
    }
}

class JournalPlayer {
    public function __construct($file, $store, $position, $bucketName = NULL) {
        $this->f = fopen($file, 'r');
        $this->store = $store;
        $this->bucketName = $bucketName;
        fseek($this->f, $position, SEEK_SET);
    }

    public function getPosition() {
        return ftell($this->f);
    }

    public function playOne() {
        if (($line = fgets($this->f)) === false)
            return false;
        $line = rtrim($line);
        $entry = unserialize($line);
        if ($this->bucketName !== NULL && $entry[2] != $this->bucketName)
            return;
        $bucket = $this->store->getBucket($entry[2]);
        switch ($entry[0]) {
        case 'insert':
            $bucket->insert($entry[3], $entry[1]);
            break;
        case 'remove':
            $bucket->remove($entry[3], $entry[1]);
            break;
        }
        return true;
    }

    public function play() {
        while ($this->playOne());
    }
}

class BucketContainer {
    protected $data;
    protected $index;

    public function __construct() {
        $this->data = array();
        $this->index = array();
    }

    public function __sleep() {
        return array('data', 'index');
    }

    protected function index(&$datum) {
        foreach ($datum as $fieldName => $fieldData) {
            $bags = &$this->index[$fieldName];
            if (!isset($bags))
                $bags = array();
            $h = substr($fieldData, 0, 16);
            $bag = &$bags[$h];
            if (!isset($bag))
                $bag = array();
            $bag[$datum['_id']] = &$datum;
        }
    }

    protected function deindex($id) {
        $datum = &$this->data[$id];
        foreach ($datum as $fieldName => $fieldData) {
            $bags = &$this->index[$fieldName];
            if (!isset($bags))
                continue;
            $h = substr($fieldData, 0, 16);
            $bag = &$bags[$h];
            if (isset($bag))
                continue;
            unset($bag[$datum['_id']]);
        }
    }

    public function find($cond) {
        $idSet = array();
        foreach ($cond as $fieldName => $fieldData) {
            if (isset($this->index[$fieldName])) {
                $bags = $this->index[$fieldName];
                $h = substr($fieldData, 0, 16);
                if (isset($bags[$h])) {
                    $bag = $bags[$h];
                    foreach ($bag as &$datum) {
                        if ($datum[$fieldName] == $fieldData)
                            $idSet[$datum['_id']] = true;
                    }
                }
            }
        }
        $retval = array();
        foreach ($idSet as $id => $_)
            $retval[] = &$this->data[$id]; 
        return $retval;
    }

    public function remove($id) {
        $this->deindex($id);
        unset($this->data[$id]);
    }

    public function insert(&$datum) {
        $id = isset($datum['_id']) ? $datum['_id']: NULL;
        if ($id === NULL) {
            $this->data[] = &$datum;
            end($this->data);
            $datum['_id'] = $id = key($this->data);
        } else {
            $this->deindex($id);
            $this->data[$id] = &$datum;
        }
        $this->index($datum);
        return $id;
    }

}

class Bucket {
    public $store;
    public $name;
    public $datafile;
    protected $container;
  
    public function __construct($store, $name, $datafile) {
        $this->store = $store;
        $this->name = $name;
        $this->datafile = $datafile;
        $this->container = new BucketContainer();
    }

    public function find($cond) {
        return $this->container->find($cond);
    }

    public function remove($id, $now) {
        return $this->container->remove($id);
    }

    public function insert(&$datum, $now) {
        return $this->container->insert($datum);
    }

    public function save() {
        $data = serialize($this->container);
        if (file_put_contents($this->datafile, $data) !== strlen($data))
            throw new IOError("Could not write the snapshot to \"{$this->datafile}\"");
    }

    public function load() {
        if (($data = @file_get_contents($this->datafile)) === false)
            throw new IOError("Could not open file \"{$this->datafile}\"");
        $container = unserialize($data);
        if ($container === false)
            throw new IOError("Could not restore the bucket from \"{$this->datafile}\"");
        $this->container = $container;
    }
}

class DataStore {
    protected $dbpath;
    protected $buckets;

    public function __construct($dbpath) {
        $this->dbpath = $dbpath;
        $this->buckets = array();
    }

    public function getBucket($bucketName) {
        $bucket = &$this->buckets[$bucketName];
        if (!isset($bucket)) {
            $datafile = $this->dbpath . '/'. rawurlencode($bucketName). '.dat';
            $bucket = new Bucket($this, $bucketName, $datafile);
        }
        return $bucket;
    }
}

class BucketProxy {
    protected $bucket;
    protected $journal;
    protected $lastSavedLogPosition;
    protected $metafile;

    public function __construct($bucket, $journal) {
        $this->bucket = $bucket;
        $this->journal = $journal;
        $this->metafile = $this->bucket->datafile. '.meta';
    }

    public function find($cond) {
        return $this->bucket->find($cond);
    }

    public function remove($id, $now) {
        $this->journal->remove($this->bucket, $id, $now); 
        return $this->bucket->remove($id, $now);
    }

    public function insert(&$datum, $now) {
        $this->journal->insert($this->bucket, $datum, $now); 
        return $this->bucket->insert($datum, $now);
    }

    public function save() {
        $logPosition = $this->journal->getPosition();
        if ($this->lastSavedLogPosition === NULL
                || $logPosition > $this->lastSavedLogPosition) {
            $meta = array('logPosition' => $logPosition);
            if (!file_put_contents($this->metafile, serialize($meta)))
                throw new IOError("Could not write to \"{$this->metafile}\"");
            $this->bucket->save();
            $this->lastSavedLogPosition = $logPosition;
        }
    }

    public function load() {
        if (!file_exists($this->metafile)) {
            return;
        }
        if (($serializedMeta = file_get_contents($this->metafile)) === false)
            throw new IOError("Could not read from \"{$this->metafile}\"");
        $meta = unserialize($serializedMeta);
        $this->bucket->load();
        $player = new JournalPlayer(
            $this->journal->file,
            $this->bucket->store,
            $meta['logPosition'],
            $this->bucket->name);
        $player->play();
        $this->lastSavedLogPosition = $meta['logPosition'];
    }
}

class DataStoreProxy {
    protected $reactor;
    protected $store;
    protected $journal;
    protected $bucketProxies;
    protected $metafile;

    public function __construct($reactor, $store, $journal, $metafile, $interval = 30.) {
        $this->reactor = $reactor;
        $this->store = $store;
        $this->journal = $journal;
        $this->metafile = $metafile;
        $this->interval = $interval;
        $this->bucketProxies = array();
        $this->reactor->addTimeoutHandler(microtime(true) + $this->interval, $this);
    }

    public function getBucket($bucketName) {
        $bucketProxy = &$this->bucketProxies[$bucketName];
        if (!isset($bucketProxy)) {
            $bucket = $this->store->getBucket($bucketName);
            $bucketProxy = new BucketProxy($bucket, $this->journal);
            $this->saveMetaFile();
        }
        return $bucketProxy;
    }

    protected function saveMetaFile() {
        $serializedMeta = serialize(array('buckets' => array_keys($this->bucketProxies)));
        if (file_put_contents($this->metafile, $serializedMeta) !== strlen($serializedMeta))
            throw new IOError("Could not write the metadata to \"{$this->metafile}\"");
    }

    public function save() {
        foreach ($this->bucketProxies as $bucketProxy) {
            try {
                $bucketProxy->save();
            } catch (IOError $e) {
                var_dump($e);
            }
        }
    }

    public function load() {
        if (($serializedMeta = @file_get_contents($this->metafile)) === false)
            return;

        $meta = unserialize($serializedMeta);
        foreach ($meta['buckets'] as $bucketName) {
            $bucketProxy = $this->getBucket($bucketName);
            $bucketProxy->load();
        }
    }

    public function onTimeout($now) {
        $this->save();
        $this->reactor->addTimeoutHandler($now + $this->interval, $this);
    }
}

class PUTHandler {
    public function __construct($processor, $prevReader, $bucketName) {
        $this->processor = $processor;
        $this->prevReader = $prevReader;
        $this->bucketName = $bucketName;
    }

    public function onDataReceived($fixedLengthDataReader, $json, $now) {
        $this->prevReader->start($now);
        $datum = json_decode($json, true);
        if ($datum == null) {
            $writer = new Sender(
                $this->processor->reactor,
                $this->processor->ringbuf,
                "ERROR\n");
            $writer->start($now);
            return;
        }
        $bucket = $this->processor->server->store->getBucket($this->bucketName);
        $id = $bucket->insert($datum, $now);
        $writer = new Sender(
            $this->processor->reactor,
            $this->processor->ringbuf,
            "OK ". $id. "\n");
        $writer->start($now);
    }

    public function onShutdown($now) {
        $this->processor->onShutdown($now);
    }
}

class GETHandler {
    public function __construct($processor, $prevReader, $bucketName) {
        $this->processor = $processor;
        $this->prevReader = $prevReader;
        $this->bucketName = $bucketName;
    }

    public function onDataReceived($fixedLengthDataReader, $json, $now) {
        $this->prevReader->start($now);
        $cond = json_decode($json, true);
        if ($cond == null) {
            $writer = new Sender(
                $this->processor->reactor,
                $this->processor->ringbuf,
                "ERROR\n");
            $writer->start($now);
            return;
        }

        $bucket = $this->processor->server->store->getBucket($this->bucketName);
        $data = $bucket->find($cond);

        $result = json_encode($data);
        $writer = new Sender(
            $this->processor->reactor,
            $this->processor->ringbuf,
            sprintf("OK %d\n", strlen($result)),
            new Sender(
                $this->processor->reactor,
                $this->processor->ringbuf,
                $result
            )
        );
        $writer->start($now);
    }

    public function onShutdown($now) {
        $this->processor->onShutdown($now);
    }
}

class CommandProcessor {
    public $server;
    public $reactor;
    public $ringbuf;
    
    public function __construct($server, $reactor, $ringbuf) {
        $this->server = $server;
        $this->reactor = $reactor;
        $this->ringbuf = $ringbuf;
    }

    public function onDataReceived($lineReader, $line, $now) {
        $args = preg_split('/\\s+/', $line);
        $cmd = strtoupper(array_shift($args));
        switch ($cmd) {
        case 'PUT':
            if (count($args) != 2) {
                $writer = new Sender($this->reactor, $this->ringbuf, "ERROR ARGUMENT\n");
                $writer->start($now);
                break;
            }
            $bucketName = $args[0];
            $dataLength = (int)$args[1];
            $reader = new FixedLengthDataReader(
                $this->reactor,
                $this->ringbuf,
                $dataLength,
                new PUTHandler($this, $lineReader, $bucketName)
            );
            $reader->start($now);
            break;
        case 'GET':
            if (count($args) != 2) {
                $writer = new Sender($this->reactor, $this->ringbuf, "ERROR ARGUMENT\n");
                $writer->start($now);
                break;
            }
            $bucketName = $args[0];
            $dataLength = (int)$args[1];
            $reader = new FixedLengthDataReader(
                $this->reactor,
                $this->ringbuf,
                $dataLength,
                new GETHandler($this, $lineReader, $bucketName)
            );
            $reader->start($now);
            break;
        case '':
            $writer = new Sender(
                $this->reactor,
                $this->ringbuf,
                "OK\n");
            $writer->start($now);
            break;
        default:
            $writer = new Sender($this->reactor, $this->ringbuf, "ERROR UNKNOWN COMMAND\n");
            $writer->start($now);
        }
    }

    public function onShutdown($now) {
        fclose($this->ringbuf->sock);
        $this->reactor->removeHandler('read', $this->ringbuf->sock);
        $this->reactor->removeHandler('write', $this->ringbuf->sock);
    }
}

class Sender {
    public function __construct($reactor, $ringbuf, $buf, $next = NULL) {
        $this->reactor = $reactor;
        $this->ringbuf = $ringbuf;
        $this->buf = $buf;
        $this->next = $next;
    }

    public function start($now) {
        if ($this->ringbuf->write($this->buf) == strlen($this->buf)) {
            if ($this->next)
                $this->next->onDataSent($this, $now);
        } else {
            $this->reactor->addHandler('write', $this->ringbuf->sock, $this);
        }
    }

    public function onWriteReady($reactor, $fd, $now) {
        if ($this->ringbuf->write() == 0) {
            $this->reactor->removeHandler('write', $this->ringbuf->sock);
            if ($this->next)
                $this->next->onDataSent($this, $now);
        }
    }

    public function onDataSent($prevSender, $now) {
        $this->start($now);
    }
}

class FixedLengthDataReader {
    public $reactor;
    public $client;
    public $next;
    public $buf;

    public function __construct($reactor, $ringbuf, $length, $next) {
        $this->reactor = $reactor;
        $this->ringbuf = $ringbuf;
        $this->length = $length;
        $this->next = $next;
        $this->buf = '';
    }

    public function start($now) {
        $this->reactor->addHandler('read', $this->ringbuf->sock, $this);
        $this->onReadReady($this->reactor, NULL, $now);
    }

    public function onReadReady($reactor, $fd, $now) {
        if (strlen($this->buf) == $this->length) {
            $this->next->onDataReceived($this, $this->buf, $now); 
        } else {
            $chunk = $this->ringbuf->read($this->length - strlen($this->buf), $fd === NULL);
            if ($this->ringbuf->eof) {
                $this->next->onShutdown($now);
                return;
            }
            $this->buf .= $chunk;
            if (strlen($this->buf) == $this->length) {
                $this->next->onDataReceived($this, $this->buf, $now);
            }
        }
    }
}

class LineReader {
    public $reactor;
    public $ringbuf;
    public $next;
    public $buf;

    public function __construct($reactor, $ringbuf, $next) {
        $this->reactor = $reactor;
        $this->ringbuf = $ringbuf;
        $this->next = $next;
        $this->buf = '';
    }

    public function start($now) {
        $this->reactor->addHandler('read', $this->ringbuf->sock, $this);
        $this->onReadReady($this->reactor, NULL, $now);
    }

    public function onReadReady($reactor, $fd, $now) {
        $chunk = $this->ringbuf->read(4096, $fd === NULL);
        if ($this->ringbuf->eof) {
            $this->next->onShutdown($now);
            return;
        }
        $this->buf .= $chunk;
        $pair = preg_split('/\r\n|\r|\n/', $this->buf, 2);
        if (count($pair) == 2) {
            $this->ringbuf->unread($pair[1]);
            $this->buf = '';
            $this->next->onDataReceived($this, $pair[0], $now);
        }
    }
}

class RingBuffer {
    public $sock;
    public $eof;
    protected $readBuf;
    protected $writeBuf;

    public function __construct($sock) {
        $this->sock = $sock;
        $this->readBuf = '';
        $this->writeBuf = '';
        $this->eof = false;
    }

    public function read($nbytes, $buffered = false) {
        $nbytesConsumerFromBuffer = min($nbytes, strlen($this->readBuf));
        $retval = substr($this->readBuf, 0, $nbytes);
        $this->readBuf = substr($this->readBuf, $nbytes);
        $nbytes -= $nbytesConsumerFromBuffer;
        if (!$buffered && strlen($this->readBuf) < $nbytes) {
            if (($chunk = fread($this->sock, $nbytes)) === false) {
                throw new IOError();
            }
            if ($chunk == '')
                $this->eof = true;
            $retval = $this->readBuf;
            $retval .= $chunk;
            $this->readBuf = '';
        }
        return $retval;
    }

    public function unread($data) {
        $this->readBuf .= $data;
    }

    public function write($data = '') {
        if ($this->writeBuf == '')
            $this->writeBuf = $data;
        if (strlen($this->writeBuf) == 0)
            return 0;
        if (($nbytes = fwrite($this->sock, $this->writeBuf)) === false) {
            throw new IOError();
        }
        $this->writeBuf = substr($this->writeBuf, $nbytes);
        return $nbytes;
    }
}

class Reactor {
    protected $fds;
    protected $fn;
    protected $timeouts;
    protected $nearestTimeoutValue;

    public function __construct() {
        $this->fds = array('read' => array(), 'write' => array());
        $this->fn = array('read' => array(), 'write' => array());
        $this->timeouts = array();
    }

    public function addHandler($ev, $sock, $fn) {
        if (!isset($this->fn[$ev][(int)$sock]))
            $this->fds[$ev][] = $sock;
        $this->fn[$ev][(int)$sock] = $fn;
    }

    public function removeHandler($ev, $sock) {
        if (isset($this->fn[$ev][(int)$sock])) {
            unset($this->fn[$ev][(int)$sock]);
            $fds = &$this->fds[$ev];
            $i = array_search($sock, $fds);
            if ($i !== false)
                array_splice($fds, $i, 1);
        }
    }

    public function addTimeoutHandler($time, $fn) {
        $time = (float)$time;
        $this->timeouts[] = array($time, $fn);
    }

    public function run() {
        for (;;) {
            $now = microtime(true);
            $rfds = $this->fds['read'];
            $wfds = $this->fds['write'];
            $timeouts = $this->timeouts;
            $this->timeouts = array();
            usort($timeouts, function ($a, $b) {
                return $a[0] > $b[0] ? 1: ($a[0] < $b[0] ? -1: 0);
            });
            $wait = $timeouts ? $timeouts[0][0] - $now: 1;
            if (!$rfds && !$wfds) {
                $n = 0;
                usleep($wait * 1000000);
            } else {
                $efds = NULL;
                $wait_sec = max((int)$wait, 0.);
                $wait_usec = max(($wait - (float)$wait_sec) * 1000000., 0.);
                $n = stream_select($rfds, $wfds, $efds, $wait_sec, $wait_usec);
            }
            if ($timeouts) {
                $now = microtime(true);
                foreach ($timeouts as $i => $timeout) {
                    if ($timeout[0] < $now) {
                        unset($timeouts[$i]);
                        $timeout[1]->onTimeout($now);
                    } else {
                        break;
                    }
                }
            }
            $this->timeouts = array_merge($this->timeouts, $timeouts);
            if ($n === false)
                continue;
            if ($n === 0)
                continue;
            foreach ($rfds as $fd) {
                $fn = $this->fn['read'][(int)$fd];
                $fn and $fn->onReadReady($this, $fd, $now);
            }
            foreach ($wfds as $fd) {
                $fn = $this->fn['write'][(int)$fd];
                $fn and $fn->onWriteReady($this, $fd, $now);
            }
        }
    }
}

class Server {
    protected $ssock;
    protected $_store;
    protected $journal;
    public $store;
    public $reactor;

    public function __construct($reactor, $addr = null, $port = 50129) {
        if (($ssock = stream_socket_server(sprintf('tcp://%s:%d', $addr ?: "0.0.0.0", $port), $errno, $errstr)) === false)
            throw new IOError($errno, $errstr);
        stream_set_blocking($ssock, false);
        $this->ssock = $ssock;
        $this->journal = new Journal('./uguisudani.jnl');
        $this->_store = new DataStore('.');
        $this->store = new DataStoreProxy($reactor, $this->_store, $this->journal, './uguisudani.meta');
        $this->reactor = $reactor;
        $this->reactor->addHandler('read', $ssock, $this);
        $this->store->load();
    }

    public function onReadReady($reactor, $fd, $now) {
        $csock = stream_socket_accept($this->ssock);
        stream_set_blocking($csock, false);
        $ringbuf = new RingBuffer($csock);
        $reader = new LineReader(
            $this->reactor,
            $ringbuf,
            new CommandProcessor($this, $reactor, $ringbuf)
        );
        $reader->start($now);
    }
}

$reactor = new Reactor();
$server = new Server($reactor, 'localhost');
$reactor->run();
?>
