import subprocess

subprocess.call("data_processer.py",shell = True)
subprocess.call("data_analyzer_with_tag.py",shell = True)
subprocess.call("make_user_local_process.py",shell = True)
subprocess.call("make_csvfile_from_user_local_process_with_tag",shell = True)
