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
    "cachePath" : "/tmp/test/"
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
    "cachePath" : "/tmp/dev/"
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
    "cachePath" : "/tmp/prod/"    
  }
}