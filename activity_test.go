package CassandraQuery

import (
	"io/ioutil"
	"testing"
	//"strconv"

	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-contrib/action/flow/test"
	"github.com/stretchr/testify/assert"
	 
	
)

var activityMetadata *activity.Metadata

func getActivityMetadata() *activity.Metadata {

	if activityMetadata == nil {
		jsonMetadataBytes, err := ioutil.ReadFile("activity.json")
		if err != nil{
			panic("No Json Metadata found for activity.json path")
		}

		activityMetadata = activity.NewMetadata(string(jsonMetadataBytes))
	}

	return activityMetadata
}

func TestCreate(t *testing.T) {

	act := NewActivity(getActivityMetadata())

	if act == nil {
		t.Error("Activity Not Created")
		t.Fail()
		return
	}
}

func TestEval(t *testing.T) {

	defer func() {
		if r := recover(); r != nil {
			t.Failed()
			t.Errorf("panic during execution: %v", r)
		}
	}()

	act := NewActivity(getActivityMetadata())
	tc := test.NewTestActivityContext(getActivityMetadata())

	//setup attrs
	tc.SetInput("ClusterIP", "127.0.0.1")
	tc.SetInput("Keyspace", "sample")
	tc.SetInput("TableName", "employee")
	
//	var(
//		empid int
//		name string
//		salary float64			
//	)
	act.Eval(tc)

//	tempID := strconv.Itoa(empid)
	//tsalary := strconv.ParseFloat(salary, 64);
	//tsalary := floattostr(salary)
//	tsalary := strconv.FormatFloat(salary, 'f', 2, 64)
	
	//check result attr
	result := tc.GetOutput("result")
	assert.Equal(t,result,("EmpID: 103 Name: pqr Salary: 7000.50"))
}
