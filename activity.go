package CassandraQuery

import (
	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-lib/logger"
	 "github.com/gocql/gocql"
	//"fmt"
	"strconv"
	
)
	// THIS IS ADDED
	// log is the default package logger which we'll use to log
	var log = logger.GetLogger("activity-CassaSample")

// MyActivity is a stub for your Activity implementation
type MyActivity struct {
	metadata *activity.Metadata
}

// NewActivity creates a new activity
func NewActivity(metadata *activity.Metadata) activity.Activity {
	return &MyActivity{metadata: metadata}
}

// Metadata implements activity.Activity.Metadata
func (a *MyActivity) Metadata() *activity.Metadata {
	return a.metadata
}

// Eval implements activity.Activity.Eval
func (a *MyActivity) Eval(context activity.Context) (done bool, err error)  {
	// Get the activity data from the context
		clusterIP := context.GetInput("ClusterIP").(string)
		keySpace := context.GetInput("Keyspace").(string)
		tableName := context.GetInput("TableName").(string)

	// Use the log object to log the greeting
	log.Debugf("The Flogo engine says [%s] to [%s] with table [%s]",clusterIP,keySpace,tableName)

	 // Provide the cassandra cluster instance here.
    cluster := gocql.NewCluster(clusterIP) 
 
    // gocql requires the keyspace to be provided before the session is created.
    // In future there might be provisions to do this later.
    cluster.Keyspace = keySpace 
    
 
   // cluster.ProtoVersion = 4
    session, err := cluster.CreateSession()
	log.Debugf("Session Created Sucessfully")
	
	var(
		empid int
		name string
		salary float64			
	) 
	
	iter := session.Query("SELECT empid, name, salary FROM "+tableName).Iter()
    for iter.Scan(&empid , &name, &salary) {
       // fmt.Println("EmpID: ", empid,"Name: ", name , "Salary: ", salary)
        
    }
	if err := iter.Close(); err != nil {
		log.Debugf("Error")		
	}
	
	if err != nil {
       log.Debugf("Could not connect to cassandra cluster: ", err)
   }
	log.Debugf("Session : " , session)
	log.Debugf("Cluster : " , clusterIP)
	log.Debugf("Keyspace : ", keySpace)
	log.Debugf("Session Timeout : " ,cluster.Timeout)	
    
	tempID := strconv.Itoa(empid)
	//tsalary := strconv.ParseFloat(salary, 64);
	//tsalary := floattostr(salary)
	tsalary := strconv.FormatFloat(salary, 'f', 2, 64)

	// Set the result as part of the context
	context.SetOutput("result",("EmpID: "+tempID+" Name: "+name +" Salary: "+tsalary))

	// Signal to the Flogo engine that the activity is completed
	return true, nil
}
