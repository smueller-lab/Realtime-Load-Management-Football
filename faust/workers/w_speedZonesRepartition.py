import faust

app = faust.App('master', broker='kafka://localhost:29092')