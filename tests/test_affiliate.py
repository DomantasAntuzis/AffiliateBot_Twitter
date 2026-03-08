import pytest

from services.affiliate_service import *

# mock the 'config' and 'logger' so they don't cause errors
@pytest.fixture(autouse=True)
def mock_dependencies():
    with patch('your_module.config') as mock_config, \
         patch('your_module.logger') as mock_logger:
        mock_config.TEMP_DIR = "fake_dir"
        yield mock_config, mock_logger

def test_fetch_cj_data_files():


