package database

import (
	"bosun.org/_third_party/github.com/garyburd/redigo/redis"
	"bosun.org/collect"
	"bosun.org/models"
	"bosun.org/opentsdb"
	"encoding/json"
	"fmt"
)

/*

failingAlerts = set of currently failing alerts
alertsWithErrors = set of alerts with any errors
errorEvents = list of (alert) one per individual error event
error:{name} = list of json objects for coalesced error events (most recent first).

*/

type ErrorDataAccess interface {
	MarkAlertSuccess(name string) error
	MarkAlertFailure(name string) error
	GetFailingAlertCounts() (int, int, error)

	GetFailingAlerts() (map[string]bool, error)
	IsAlertFailing(name string) (bool, error)

	GetLastEvent(name string) (*models.AlertError, error)
	UpdateLastEvent(name string, event *models.AlertError) error
	AddEvent(name string, event *models.AlertError) error

	GetFullErrorHistory() (map[string][]*models.AlertError, error)
	ClearAlert(name string) error
	ClearAll() error
}

func (d *dataAccess) Errors() ErrorDataAccess {
	return d
}

const (
	failingAlerts    = "failingAlerts"
	errorEvents      = "errorEvents"
	alertsWithErrors = "alertsWithErrors"
)

func (d *dataAccess) MarkAlertSuccess(name string) error {
	defer collect.StartTimer("redis", opentsdb.TagSet{"op": "MarkAlertSuccess"})()
	conn := d.GetConnection()
	defer conn.Close()
	_, err := conn.Do("SREM", failingAlerts, name)
	return err
}

func (d *dataAccess) MarkAlertFailure(name string) error {
	defer collect.StartTimer("redis", opentsdb.TagSet{"op": "MarkAlertFailure"})()
	conn := d.GetConnection()
	defer conn.Close()
	if _, err := conn.Do("SADD", alertsWithErrors, name); err != nil {
		return err
	}
	_, err := conn.Do("SADD", failingAlerts, name)
	return err
}

func (d *dataAccess) GetFailingAlertCounts() (int, int, error) {
	defer collect.StartTimer("redis", opentsdb.TagSet{"op": "GetFailingAlertCounts"})()
	conn := d.GetConnection()
	defer conn.Close()
	failing, err := redis.Int(conn.Do("SCARD", failingAlerts))
	if err != nil {
		return 0, 0, err
	}
	events, err := redis.Int(conn.Do("LLEN", errorEvents))
	if err != nil {
		return 0, 0, err
	}
	return failing, events, nil
}

func (d *dataAccess) GetFailingAlerts() (map[string]bool, error) {
	defer collect.StartTimer("redis", opentsdb.TagSet{"op": "GetFailingAlertCounts"})()
	conn := d.GetConnection()
	defer conn.Close()
	alerts, err := redis.Strings(conn.Do("SMEMBERS", failingAlerts))
	if err != nil {
		return nil, err
	}
	r := make(map[string]bool, len(alerts))
	for _, a := range alerts {
		r[a] = true
	}
	return r, nil
}
func (d *dataAccess) IsAlertFailing(name string) (bool, error) {
	defer collect.StartTimer("redis", opentsdb.TagSet{"op": "IsAlertFailing"})()
	conn := d.GetConnection()
	defer conn.Close()
	return redis.Bool(conn.Do("SISMEMBER", failingAlerts, name))
}

func errorListKey(name string) string {
	return fmt.Sprintf("errors:%s", name)
}
func (d *dataAccess) GetLastEvent(name string) (*models.AlertError, error) {
	defer collect.StartTimer("redis", opentsdb.TagSet{"op": "GetLastEvent"})()
	conn := d.GetConnection()
	defer conn.Close()

	str, err := redis.Bytes(conn.Do("LINDEX", errorListKey(name), "0"))
	if err != nil {
		if err == redis.ErrNil {
			return nil, nil
		}
		return nil, err
	}
	ev := &models.AlertError{}
	if err = json.Unmarshal(str, ev); err != nil {
		return nil, err
	}
	return ev, nil
}

func (d *dataAccess) UpdateLastEvent(name string, event *models.AlertError) error {
	defer collect.StartTimer("redis", opentsdb.TagSet{"op": "IncrementLastEvent"})()
	conn := d.GetConnection()
	defer conn.Close()
	marshalled, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = conn.Do("LPOP", errorListKey(name))
	if err != nil {
		return err
	}
	_, err = conn.Do("LPUSH", errorListKey(name), marshalled)
	if err != nil {
		return err
	}
	_, err = conn.Do("LPUSH", errorEvents, name)
	return err
}

func (d *dataAccess) AddEvent(name string, event *models.AlertError) error {
	defer collect.StartTimer("redis", opentsdb.TagSet{"op": "AddEvent"})()
	conn := d.GetConnection()
	defer conn.Close()

	marshalled, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = conn.Do("LPUSH", errorListKey(name), marshalled)
	if err != nil {
		return err
	}
	_, err = conn.Do("LPUSH", errorEvents, name)
	return err
}

func (d *dataAccess) GetFullErrorHistory() (map[string][]*models.AlertError, error) {
	defer collect.StartTimer("redis", opentsdb.TagSet{"op": "GetFullErrorHistory"})()
	conn := d.GetConnection()
	defer conn.Close()

	alerts, err := redis.Strings(conn.Do("SMEMBERS", alertsWithErrors))
	if err != nil {
		return nil, err
	}
	results := make(map[string][]*models.AlertError, len(alerts))
	for _, a := range alerts {
		rows, err := redis.Strings(conn.Do("LRANGE", errorListKey(a), 0, -1))
		if err != nil {
			return nil, err
		}
		list := make([]*models.AlertError, len(rows))
		for i, row := range rows {
			ae := &models.AlertError{}
			err = json.Unmarshal([]byte(row), ae)
			if err != nil {
				return nil, err
			}
			list[i] = ae
		}
		results[a] = list
	}
	return results, nil
}

func (d *dataAccess) ClearAlert(name string) error {
	defer collect.StartTimer("redis", opentsdb.TagSet{"op": "ClearAlert"})()
	conn := d.GetConnection()
	defer conn.Close()

	_, err := conn.Do("SREM", alertsWithErrors, name)
	if err != nil {
		return err
	}
	_, err = conn.Do("SREM", failingAlerts, name)
	if err != nil {
		return err
	}
	_, err = conn.Do("DEL", errorListKey(name))
	if err != nil {
		return err
	}
	_, err = conn.Do("LREM", errorEvents, 0, name)
	if err != nil {
		return err
	}
	return nil
}

func (d *dataAccess) ClearAll() error {
	defer collect.StartTimer("redis", opentsdb.TagSet{"op": "ClearAll"})()
	conn := d.GetConnection()
	defer conn.Close()

	alerts, err := redis.Strings(conn.Do("SMEMBERS", alertsWithErrors))
	if err != nil {
		return err
	}
	for _, a := range alerts {
		if _, err := conn.Do("DEL", errorListKey(a)); err != nil {
			return err
		}
	}
	if _, err := conn.Do("DEL", alertsWithErrors); err != nil {
		return err
	}
	if _, err := conn.Do("DEL", failingAlerts); err != nil {
		return err
	}
	if _, err = conn.Do("DEL", errorEvents); err != nil {
		return err
	}

	return nil
}
