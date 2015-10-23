package database

import (
	"bosun.org/collect"
	"bosun.org/opentsdb"
)

/*

failingAlerts = set of currently failing alerts
errorEvents = list of (alert:timestamp pairs) one per individual error event
error:{name} = list of json objects for coalesced error events (most recent first).

*/

func (d *dataAccess) Errors() ErrorDataAccess {
	return d
}

func (d *dataAccess) MarkAlertSuccess(name string) error {
	defer collect.StartTimer("redis", opentsdb.TagSet{"op": "MarkAlertSuccess"})()
	conn := d.GetConnection()
	defer conn.Close()
	_, err := conn.Do("SREM", "failingAlerts", name)
	return err
}

func (d *dataAccess) MarkAlertFailure(name string) error {
	defer collect.StartTimer("redis", opentsdb.TagSet{"op": "MarkAlertFailure"})()
	conn := d.GetConnection()
	defer conn.Close()
	_, err := conn.Do("SADD", "failingAlerts", name)
	return err
}
