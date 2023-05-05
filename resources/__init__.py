import os

# 获取同目录下的test-config.yaml文件的绝对路径
test_config_file_path = os.path.join(os.path.dirname(__file__), 'test-config.yaml')
test_vcr_tmp_file_dir_path = os.path.join(os.path.dirname(__file__), '__vcr_cassettes__')