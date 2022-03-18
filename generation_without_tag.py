import subprocess

subprocess.call("data_analyzer_without_tag.py",shell = True)
subprocess.call("make_user_local_process.py",shell = True)
subprocess.call("make_csvfile_from_user_local_process_without_tag",shell = True)