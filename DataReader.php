<?php

    namespace yii\vertica;

    use yii\base\InvalidCallException;

    class DataReader  extends \yii\base\Object implements \Iterator, \Countable {

        /**
         * @var resource the PDOStatement associated with the command
         */
        private $_statement;
        private $_connect;
        private $_closed = false;
        private $_row;
        private $_index = -1;


        /**
         * Constructor.
         * @param Command $command the command generating the query result
         * @param array $config name-value pairs that will be used to initialize the object properties
         */
        public function __construct(Command $command, $config = [])
        {
            $this->_statement = $command->db->resource();
            $this->_connect = $command->db->activeConnect;
            parent::__construct($config);
        }

//        /**
//         * Binds a column to a PHP variable.
//         * When rows of data are being fetched, the corresponding column value
//         * will be set in the variable. Note, the fetch mode must include PDO::FETCH_BOUND.
//         * @param integer|string $column Number of the column (1-indexed) or name of the column
//         * in the result set. If using the column name, be aware that the name
//         * should match the case of the column, as returned by the driver.
//         * @param mixed $value Name of the PHP variable to which the column will be bound.
//         * @param integer $dataType Data type of the parameter
//         * @see http://www.php.net/manual/en/function.PDOStatement-bindColumn.php
//         */
//        public function bindColumn($column, &$value, $dataType = null)
//        {
//            if ($dataType === null) {
//                $this->_statement->bindColumn($column, $value);
//            } else {
//                $this->_statement->bindColumn($column, $value, $dataType);
//            }
//        }

        /**
         * Set the default fetch mode for this statement
         * @param integer $mode fetch mode
         * @see http://www.php.net/manual/en/function.PDOStatement-setFetchMode.php
         */
        public function setFetchMode($mode)
        {
            $params = func_get_args();
            call_user_func_array([$this->_statement, 'setFetchMode'], $params);
        }

        /**
         * Advances the reader to the next row in a result set.
         * @return array the current row, false if no more row available
         */
        public function read()
        {
            return odbc_fetch_array($this->_statement);
        }

        /**
         * Returns a single column from the next row of a result set.
         * @param integer $columnIndex zero-based column index
         * @return mixed the column of the current row, false if no more rows available
         */
        public function readColumn($columnIndex)
        {
            $fieldName = odbc_field_name($this->_statement, $columnIndex);
            $r = odbc_fetch_array($this->_statement);
            if (!empty($fieldName) && !empty($r)) {
                return $r[ $fieldName ];
            }
            return false;
        }

        /**
         * Returns an object populated with the next row of data.
         * @param string $className class name of the object to be created and populated
         * @param array $fields Elements of this array are passed to the constructor
         * @return mixed the populated object, false if no more row of data available
         */
        public function readObject($className, $fields)
        {
            return odbc_fetch_object($this->_statement);
        }

        /**
         * Reads the whole result set into an array.
         * @return array the result set (each array element represents a row of data).
         * An empty array will be returned if the result contains no row.
         */
        public function readAll()
        {
            $result = [];
            while ($row = odbc_fetch_array($this->_statement)) {
                $result[] = $row;
            }
            return $result;
        }

        /**
         * Advances the reader to the next result when reading the results of a batch of statements.
         * This method is only useful when there are multiple result sets
         * returned by the query. Not all DBMS support this feature.
         * @return boolean Returns true on success or false on failure.
         */
        public function nextResult()
        {
            if (($result = odbc_next_result($this->_statement)) !== false) {
                $this->_index = -1;
            }

            return $result;
        }

        /**
         * Closes the reader.
         * This frees up the resources allocated for executing this SQL statement.
         * Read attempts after this method call are unpredictable.
         */
        public function close()
        {
            odbc_close($this->_connect);
            $this->_closed = true;
        }

        /**
         * whether the reader is closed or not.
         * @return boolean whether the reader is closed or not.
         */
        public function getIsClosed()
        {
            return $this->_closed;
        }

        /**
         * Returns the number of rows in the result set.
         * Note, most DBMS may not give a meaningful count.
         * In this case, use "SELECT COUNT(*) FROM tableName" to obtain the number of rows.
         * @return integer number of rows contained in the result.
         */
        public function getRowCount()
        {
            return odbc_num_rows($this->_statement);
        }

        /**
         * Returns the number of rows in the result set.
         * This method is required by the Countable interface.
         * Note, most DBMS may not give a meaningful count.
         * In this case, use "SELECT COUNT(*) FROM tableName" to obtain the number of rows.
         * @return integer number of rows contained in the result.
         */
        public function count()
        {
            return $this->getRowCount();
        }

        /**
         * Returns the number of columns in the result set.
         * Note, even there's no row in the reader, this still gives correct column number.
         * @return integer the number of columns in the result set.
         */
        public function getColumnCount()
        {
            return odbc_num_fields($this->_statement);
        }

        /**
         * Resets the iterator to the initial state.
         * This method is required by the interface [[\Iterator]].
         * @throws InvalidCallException if this method is invoked twice
         */
        public function rewind()
        {
            if ($this->_index < 0) {
                $this->_row = odbc_fetch_array($this->_statement);
                $this->_index = 0;
            } else {
                throw new InvalidCallException('DataReader cannot rewind. It is a forward-only reader.');
            }
        }

        /**
         * Returns the index of the current row.
         * This method is required by the interface [[\Iterator]].
         * @return integer the index of the current row.
         */
        public function key()
        {
            return $this->_index;
        }

        /**
         * Returns the current row.
         * This method is required by the interface [[\Iterator]].
         * @return mixed the current row.
         */
        public function current()
        {
            return $this->_row;
        }

        /**
         * Moves the internal pointer to the next row.
         * This method is required by the interface [[\Iterator]].
         */
        public function next()
        {
            $this->_row = odbc_fetch_array($this->_statement);
            $this->_index++;
        }

        /**
         * Returns whether there is a row of data at current position.
         * This method is required by the interface [[\Iterator]].
         * @return boolean whether there is a row of data at current position.
         */
        public function valid()
        {
            return $this->_row !== false;
        }
    }