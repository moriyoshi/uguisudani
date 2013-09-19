<?php
class UguisudaniException extends Exception {
}

class UguisudaniClient {
    public function __construct($sock) {
        $this->sock = $sock;
    }

    public static function connect($host, $port = 50129, $timeout = NULL) {
        if ($timeout === NULL)
            $timeout = ini_get('default_socket_timeout');
        $sock = stream_socket_client(sprintf('tcp://%s:%d', $host, $port), &$errno, &$errstr, $timeout);
        return new UguisudaniClient($sock);
    }

    protected function handleResponse($line) {
        $line = rtrim($line);
        if (substr($line, 0, 2) == 'OK')
            return substr($line, 3);
        if (substr($line, 0, 5) == 'ERROR')
            throw new UguisudaniException(substr($line, 6));
        throw new UguisudaniException("Unexpected result: ". $line);
    }

    public function put($bucket, $data) {
        $json = json_encode($data);
        fwrite($this->sock, sprintf("PUT %s %d\n", rawurlencode($bucket), strlen($json)));
        fwrite($this->sock, $json);
        return $this->handleResponse(fgets($this->sock));
    }

    public function get($bucket, $query) {
        $json = json_encode($query);
        fwrite($this->sock, sprintf("GET %s %s\n", rawurlencode($bucket), strlen($json)));
        fwrite($this->sock, $json);
        $line = fgets($this->sock);
        $nbytes = (int)$this->handleResponse($line);
        $json = fread($this->sock, $nbytes);
        $data = json_decode($json, true);
        return $data;
    }
}
