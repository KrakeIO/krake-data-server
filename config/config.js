{
  "test": {
    "postgres" :{
      "username" : "username",
      "password" : "password",
      "host" : "localhost",
      "port" : "5432",
      "database" : "scraped_data_repo_test"
    },
    "userDataDB" : "dev_panel_test",
    "cachePath" : "/tmp/test/",
    "serverPath": "http://localhost:9803"
  },  
  "development": {
    "postgres" :{
      "username" : "username",
      "password" : "password",
      "host" : "localhost",
      "port" : "5432",
      "database" : "scraped_data_repo_development"
    },
    "userDataDB" : "dev_panel_development",
    "cachePath" : "/tmp/dev/",
    "serverPath": "http://localhost:9803"
  },
  "production": {
    "postgres" : {
      "username" : "username",
      "password" : "password",
      "host" : "localhost",
      "port" : "5432",
      "database" : "scraped_data_repo"
    },
    "userDataDB" : "dev_panel",
    "cachePath" : "/tmp/krake_data_cache/",
    "serverPath": "http://data.getdata.io"
  }
}