describe "krake data server", ->
  beforeEach (done)->
    @repo_name = "1_66240a39bc8c73a3ec2a08222936fc49eses"
    done()

  it "should respond with a valid JSON http response", (done)->
    'http://localhost:9803/' + @repo_name + '/json?q={"$limit":2}'
    expect(false).toBe true
    done()