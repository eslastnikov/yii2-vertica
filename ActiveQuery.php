<?php

namespace yii\vertica;

use yii\db\ActiveQueryInterface;
use yii\db\ActiveQueryTrait;
use yii\db\ActiveRelationTrait;

class ActiveQuery extends Query implements ActiveQueryInterface
{
    use ActiveQueryTrait;
    use ActiveRelationTrait;

    /**
     * Constructor.
     * @param array $modelClass the model class associated with this query
     * @param array $config configurations to be applied to the newly created query object
     */
    public function __construct($modelClass, $config = [])
    {
        $this->modelClass = $modelClass;
        parent::__construct($config);
    }

    /**
     * Creates a DB command that can be used to execute this query.
     * @param Connection $db the DB connection used to create the DB command.
     * If null, the DB connection returned by [[modelClass]] will be used.
     * @return Command the created DB command instance.
     */
    public function createCommand($db = null)
    {
        /* @var $modelClass ActiveRecord */
        $modelClass = $this->modelClass;
        if ($db === null) {
            $db = $modelClass::getDb();
        }


        list ($sql, $params) = $db->getQueryBuilder()->build($this);

        return $db->createCommand(['sql' => $sql]);
//        return $db->createCommand(['sql' => $db->getQueryBuilder()->build($this)]);
    }

    /**
     * @inheritdoc
     */
    public function prepare($builder)
    {
        // NOTE: because the same ActiveQuery may be used to build different SQL statements
        // (e.g. by ActiveDataProvider, one for count query, the other for row data query,
        // it is important to make sure the same ActiveQuery can be used to build SQL statements
        // multiple times.
        if (!empty($this->joinWith)) {
            $this->buildJoinWith();
            $this->joinWith = null;    // clean it up to avoid issue https://github.com/yiisoft/yii2/issues/2687
        }

        if (empty($this->from)) {
            /* @var $modelClass ActiveRecord */
            $modelClass = $this->modelClass;
            $tableName = $modelClass::tableName();
            $this->from = [$tableName];
        }

        if (empty($this->select) && !empty($this->join)) {
            list(, $alias) = $this->getQueryTableName($this);
            $this->select = ["$alias.*"];
        }

        if ($this->primaryModel === null) {
            // eager loading
            $query = Query::create($this);
        } else {
            // lazy loading of a relation
            $where = $this->where;

            if ($this->via instanceof self) {
                // via junction table
                $viaModels = $this->via->findJunctionRows([$this->primaryModel]);
                $this->filterByModels($viaModels);
            } elseif (is_array($this->via)) {
                // via relation
                /* @var $viaQuery ActiveQuery */
                list($viaName, $viaQuery) = $this->via;
                if ($viaQuery->multiple) {
                    $viaModels = $viaQuery->all();
                    $this->primaryModel->populateRelation($viaName, $viaModels);
                } else {
                    $model = $viaQuery->one();
                    $this->primaryModel->populateRelation($viaName, $model);
                    $viaModels = $model === null ? [] : [$model];
                }
                $this->filterByModels($viaModels);
            } else {
                $this->filterByModels([$this->primaryModel]);
            }

            $query = Query::create($this);
            $this->where = $where;
        }

        if (!empty($this->on)) {
            $query->andWhere($this->on);
        }

        return $query;
    }

    /**
     * Executes query and returns all results as an array.
     * @param Connection $db the DB connection used to create the DB command.
     * If null, the DB connection returned by [[modelClass]] will be used.
     * @return array the query results. If the query results in nothing, an empty array will be returned.
     */
    public function all($db = null)
    {
        if ($this->asArray) {
            return parent::all($db);
        }

        $result = $this->createCommand($db)->search();

        if (empty($result)) {
            return [];
        }
        $models = $this->createModels($result);
        if (!empty($this->with)) {
            $this->findWith($this->with, $models);
        }
        foreach ($models as $model) {
            $model->afterFind();
        }

        return $models;
    }

    /**
     * Executes query and returns a single row of result.
     * @param Connection $db the DB connection used to create the DB command.
     * If null, the DB connection returned by [[modelClass]] will be used.
     * @return ActiveRecord|array|null a single row of query result. Depending on the setting of [[asArray]],
     * the query result may be either an array or an ActiveRecord object. Null will be returned
     * if the query results in nothing.
     */
    public function one($db = null)
    {
        if (($result = parent::one($db)) === false) {
            return null;
        }
        if ($this->asArray) {
            return $result;
        } else {
            /* @var $class ActiveRecord */
            $class = $this->modelClass;
            $model = $class::instantiate($result);
            $class::populateRecord($model, $result);
            if (!empty($this->with)) {
                $models = [$model];
                $this->findWith($this->with, $models);
                $model = $models[0];
            }
            $model->afterFind();
            return $model;
        }
    }
}
