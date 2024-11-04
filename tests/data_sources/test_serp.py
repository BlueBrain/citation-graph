import json

from citations.data_sources import serp


def test_create_author_id_mapping_no_files(tmp_path):
    """Test create_author_id_mapping when there are no files in the directory"""

    # Call function with temp directory as argument
    result = serp.create_author_id_mapping(tmp_path)

    # Assert that the result is an empty dictionary
    assert result == {}


def test_create_author_id_mapping_no_json_files(tmp_path):
    """Test create_author_id_mapping when there are no .json files in the directory"""

    # Create a dummy non-json file in the temp directory
    dummy_file = tmp_path / "dummy.txt"
    dummy_file.write_text("dummy content")

    # Call function with temp directory as argument
    result = serp.create_author_id_mapping(tmp_path)

    # Assert that the result is an empty dictionary
    assert result == {}


def test_create_author_id_mapping_single_json_file_single_profile(tmp_path):
    """Test create_author_id_mapping when there is a single .json file with a single profile"""

    # Create a dummy .json file in the temp directory
    dummy_json = tmp_path / "dummy.json"
    dummy_data = {"profiles": [{"author_id": "123"}]}
    dummy_json.write_text(json.dumps(dummy_data))

    # Call function with temp directory as argument
    result = serp.create_author_id_mapping(tmp_path)

    # Assert that the result matches expected
    assert result == {"dummy": "123"}


def test_create_author_id_mapping_multiple_json_files_single_profile_each(
    tmp_path,
):
    """Test create_author_id_mapping when there are multiple .json files each with a single profile"""

    # Create multiple dummy .json files in the temp directory
    for i in range(3):
        dummy_json = tmp_path / f"dummy{i}.json"
        dummy_data = {"profiles": [{"author_id": f"{i}"}]}
        dummy_json.write_text(json.dumps(dummy_data))

    # Call function with temp directory as argument
    result = serp.create_author_id_mapping(tmp_path)

    # Assert that the result matches expected
    assert result == {"dummy0": "0", "dummy1": "1", "dummy2": "2"}


def test_create_author_id_mapping_json_file_no_profile(tmp_path):
    """Test create_author_id_mapping when a .json file has no 'profiles' field"""

    # Create a dummy .json file in the temp directory with a missing "profiles" field
    dummy_json = tmp_path / "dummy.json"
    dummy_data = {}
    dummy_json.write_text(json.dumps(dummy_data))

    # Call function with temp directory as argument
    result = serp.create_author_id_mapping(tmp_path)

    # Assert that the result is an empty dictionary
    assert result == {}


def test_create_author_id_mapping_json_file_multiple_profiles(tmp_path):
    """Test create_author_id_mapping when a .json file has multiple 'profiles' fields"""

    # Create a dummy .json file in the temp directory with multiple "profiles" fields
    dummy_json = tmp_path / "dummy.json"
    dummy_data = {"profiles": [{"author_id": "123"}, {"author_id": "456"}]}
    dummy_json.write_text(json.dumps(dummy_data))

    # Call function with temp directory as argument
    result = serp.create_author_id_mapping(tmp_path)

    # Assert that the result is an empty dictionary
    assert result == {}
