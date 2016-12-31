<?php

namespace yii\vertica;

use Yii;
use yii\base\Component;
use yii\base\InvalidConfigException;

/**
 * @author Vitaliy Zarubint <keygenqt@gmail.com>
 * @since 2.0
 */
class Connection extends Component
{
    /**
     * @event Event an event that is triggered after a DB connection is established
     */
    const EVENT_AFTER_OPEN = 'afterOpen';

    /**
     * @var resource the active connect.
     */
    public $activeConnect;
    
    /**
     * @var string database name
     */
    public static $db;

    /**
     * @var string the common prefix or suffix for table names. If a table name is given
     * as `{{%TableName}}`, then the percentage character `%` will be replaced with this
     * property value. For example, `{{%post}}` becomes `{{tbl_post}}`.
     */
    public $tablePrefix = '';

    /**
     * @var string dsn connect data
     */
    public $dsn;
    
    /**
     * @var string user database vertica
     */
    public $username;
    
    /**
     * @var string password database vertica
     */
    public $password;
    
    private $_table;
    private $_resource;
    
    /**
     * @return string database name
     */
    public function getDb()
    {
        if (self::$db === null && $this->activeConnect) {
            self::$db = $this->exec('SELECT database_name FROM databases')->scalar();
        }
        return self::$db;
    }
    
    /**
     * @param string $table set name table query
     */
    public function setTable($table)
    {
        $this->_table = $table;
    }
    
    /**
     * @return string table name
     */
    public function getTable()
    {
        return $this->_table;
    }

    /**
     * @param type $sql
     *
     * @return Connection
     * @throws \Exception
     */
    public function exec($sql)
    {

        Yii::info($sql, 'yii\db\Command::query');

        Yii::beginProfile($sql, 'yii\db\Command::query');
        try {
            $this->open();
            $this->_resource = odbc_exec($this->activeConnect, $sql);
            Yii::endProfile($sql, 'yii\db\Command::query');
        } catch (\Exception $e) {
            Yii::endProfile($sql, 'yii\db\Command::query');
            throw $this->convertException($e, $sql);
        }
        return $this;
    }

    public function convertException(\Exception $e, $rawSql)
    {
        if ($e instanceof \Exception) {
            return $e;
        }

        $exceptionClass = '\yii\db\Exception';
        $message = $e->getMessage()  . "\nThe SQL being executed was: $rawSql";
        $errorInfo = null;
        return new $exceptionClass($message, $errorInfo, (int) $e->getCode(), $e);
    }

    /**
     * @param string $sql query execute
     * @param array $params not worked
     *
     * @return bool
     * @throws \Exception
     */
    public function execute($sql, $params = [])
    {
        Yii::info($sql, 'yii\db\Command::query');

        Yii::beginProfile($sql, 'yii\db\Command::query');
        try {
            $stmt = odbc_prepare($this->activeConnect, $sql);
            odbc_execute($stmt, $params);
            Yii::endProfile($sql, 'yii\db\Command::query');
        } catch (\Exception $e) {
            Yii::endProfile($sql, 'yii\db\Command::query');
            preg_match('%NOTICE 4185%i', $e->getMessage(), $matches);
            if (empty($matches)) {
                throw $this->convertException($e, $sql);
            }
        }
        return true;
    }

    /**
     * Get result query in resource
     * @return resource
     */
    public function resource()
    {
        return $this->_resource;
    }
    
    /**
     * @return array
     */
    public function one()
    {
        return odbc_fetch_array($this->_resource);
    }
    
    /**
     * @return mixed
     */
    public function scalar()
    {
        if ($value = odbc_fetch_array($this->_resource)) {
            return array_shift($value);
        }
        return null;
    }
    
    /**
     * @return array
     */
    public function all()
    {
        $result = [];
        while ($row = odbc_fetch_array($this->_resource)) {
            $result[] = $row;
        }
        return $result;
    }

    /**
     * Returns a value indicating whether the DB connection is established.
     * @return boolean whether the DB connection is established
     */
    public function getIsActive()
    {
        $this->open();
        return $this->activeConnect !== null;
    }

    /**
     * Establishes a DB connection.
     * It does nothing if a DB connection has already been established.
     * @throws \Exception if connection fails
     */
    public function open()
    {
        if ($this->activeConnect !== null) {
            return;
        }
        $token = 'Opening DB connection: ' . $this->dsn;
        try {
            Yii::info($token, __METHOD__);
            Yii::beginProfile($token, __METHOD__);
            $this->activeConnect = odbc_connect($this->dsn, $this->username, $this->password);
            $this->initConnection();
            Yii::endProfile($token, __METHOD__);
        } catch (\yii\base\ErrorException $ex) {
            Yii::endProfile($token, __METHOD__);
            throw new InvalidConfigException($ex->getMessage());
        }
    }

    /**
     * Return is connect to params
     *
     * @param string $dsn
     * @param string $username
     * @param string $password
     * @param bool $error
     *
     * @return bool
     */
    public function isConnect($dsn, $username, $password, $error = false)
    {
        try {
            odbc_connect($dsn, $username, $password);
        } catch (\yii\base\ErrorException $ex) {
            if ($error) {
                return $ex->getMessage();
            }
            return false;
        }
        return !$error;
    }

    /**
     * Closes the currently active DB connection.
     */
    public function close()
    {
        Yii::trace('Closing connection to vertica.', __CLASS__);
        odbc_close($this->activeConnect);
        $this->activeConnect = null;
    }

    /**
     * Initializes the DB connection.
     * This method is invoked right after the DB connection is established.
     * The default implementation triggers an [[EVENT_AFTER_OPEN]] event.
     */
    protected function initConnection()
    {
        $this->trigger(self::EVENT_AFTER_OPEN);
    }

    /**
     * Returns the name of the DB driver for the current [[dsn]].
     * @return string name of the DB driver
     */
    public function getDriverName()
    {
        return 'vertica';
    }

    /**
     * Processes a SQL statement by quoting table and column names that are enclosed within double brackets.
     * Tokens enclosed within double curly brackets are treated as table names, while
     * tokens enclosed within double square brackets are column names. They will be quoted accordingly.
     * Also, the percentage character "%" at the beginning or ending of a table name will be replaced
     * with [[tablePrefix]].
     * @param string $sql the SQL to be quoted
     * @return string the quoted SQL
     */
    public function quoteSql($sql)
    {
        return preg_replace_callback(
            '/(\\{\\{(%?[\w\-\. ]+%?)\\}\\}|\\[\\[([\w\-\. ]+)\\]\\])/',
            function ($matches) {
                if (isset($matches[3])) {
                    return $this->quoteColumnName($matches[3]);
                } else {
                    return str_replace('%', $this->tablePrefix, $this->quoteTableName($matches[2]));
                }
            },
            $sql
        );
    }

    /**
     * Creates a command for execution.
     *
     * @param null $sql
     *
     * @return Command the DB command
     * @internal param array $config the configuration for the Command class
     */
    public function createCommand($sql = null)
    {
        if (empty($config['db'])) {
            $this->open();
            $config['db'] = $this;
        }
        $config['sql'] = $sql;
        $command = new Command($config);

        return $command;
    }

    /**
     * Creates new query builder instance
     * @return QueryBuilder
     */
    public function getQueryBuilder()
    {
        return new QueryBuilder($this);
    }
    
    public function quoteColumnName($name)
    {
        return $name;
    }

    public function quoteTableName($name)
    {
        if (strpos($name, '(') !== false || strpos($name, '{{') !== false) {
            return $name;
        }
        if (strpos($name, '.') === false) {
            return $name;
        }
        $parts = explode('.', $name);
        foreach ($parts as $i => $part) {
            $parts[$i] = $part;
        }

        return implode('.', $parts);

//        return $name;
    }
}
