import pytest

from schematools.utils import get_dataset_prefix_from_path, to_snake_case, toCamelCase


def test_toCamelCase() -> None:
    """Confirm that:
    - space separated name is converted to camelCase
    - PascalCase results in camelCase
    - snake_case results in camelCase
    """
    assert toCamelCase("test name magic") == "testNameMagic"
    assert toCamelCase("test name magic2") == "testNameMagic2"
    assert toCamelCase("testNameMagic") == "testNameMagic"
    assert toCamelCase("TestNameMagic") == "testNameMagic"
    assert toCamelCase("test_name_magic") == "testNameMagic"
    assert toCamelCase("numbers_33_in_the_middle_44") == "numbers33InTheMiddle44"
    # mind the lower case "i" after "33". It should be made upper case
    assert toCamelCase("numbers33inTheMiddle44") == "numbers33InTheMiddle44"
    assert toCamelCase("per_jaar_per_m2") == "perJaarPerM2"

    with pytest.raises(ValueError):
        toCamelCase("")


def test_to_snake_case() -> None:
    """Confirm that:
    - space separated name converted to snake_case
    - camelCase converted to snake_case
    - snake_case converted to snake_case
    """
    assert to_snake_case("test name magic") == "test_name_magic"
    assert to_snake_case("test name magic22") == "test_name_magic_22"
    assert to_snake_case("TestNameMagic") == "test_name_magic"
    assert to_snake_case("testNameMagic") == "test_name_magic"
    assert to_snake_case("test_name_magic") == "test_name_magic"
    assert to_snake_case("hoofdroutes_u_routes") == "hoofdroutes_u_routes"
    assert to_snake_case("verlengingSluitingstijd1") == "verlenging_sluitingstijd_1"
    assert to_snake_case("numbers33inTheMiddle44") == "numbers_33_in_the_middle_44"
    assert to_snake_case("perJaarPerM2") == "per_jaar_per_m2"

    with pytest.raises(ValueError):
        to_snake_case("")


def test_get_dataset_prefix_from_path() -> None:
    """Confirm that dataset prefix can be extracted from URLs such asL
    - belastingen/precario/terrassen/terrassen => belastingen/precario
    - belastingen/precario/terrassen => belastingen/precario
    - belastingen/terrassen => belastingen
    - terrassen/terrassen => ""  # AKA old behaviour
    """
    dataset = {"id": "terrassen", "version": "0.0.1"}
    assert (
        get_dataset_prefix_from_path(
            dataset_path="belastingen/precario/terrassen/terrassen.json", dataset_data=dataset
        )
        == "belastingen/precario"
    )
    assert (
        get_dataset_prefix_from_path(
            dataset_path="belastingen/precario/terrassen.json", dataset_data=dataset
        )
        == "belastingen/precario"
    )
    assert (
        get_dataset_prefix_from_path(
            dataset_path="belastingen/terrassen.json", dataset_data=dataset
        )
        == "belastingen"
    )
    assert (
        get_dataset_prefix_from_path(dataset_path="terrassen/terrassen.json", dataset_data=dataset)
        == ""
    )


def test_get_dataset_prefix_from_path_with_versioning() -> None:
    """Confirm that dataset prefix can be extracted from URLs such as:
    - belastingen/precario/1.0.0/terrassen/terrassen => belastingen/precario
    - belastingen/precario/1.0.0/terrassen => belastingen/precario
    - belastingen/1.0.0/terrassen => belastingen
    - terrassen/1.0.0/terrassen => ""  # AKA old behaviour
    """
    dataset = {"id": "terrassen", "version": "1.0.0"}
    assert (
        get_dataset_prefix_from_path(
            dataset_path="belastingen/precario/1.0.0/terrassen/terrassen.json",
            dataset_data=dataset,
        )
        == "belastingen/precario"
    )
    assert (
        get_dataset_prefix_from_path(
            dataset_path="belastingen/precario/1.0.0/terrassen.json", dataset_data=dataset
        )
        == "belastingen/precario"
    )
    assert (
        get_dataset_prefix_from_path(
            dataset_path="belastingen/1.0.0/terrasen.json", dataset_data=dataset
        )
        == "belastingen"
    )
    assert (
        get_dataset_prefix_from_path(
            dataset_path="terrassen/1.0.0/terrassen.json", dataset_data=dataset
        )
        == ""
    )


def test_get_dataset_prefix_from_path_with_camel_case_id() -> None:
    """Confirm that dataset prefix can be extracted from URLs when dataset id is camelCase:
    - beheerkaart_cbs_grid/beheerkaart_cbs_grid.json => ""
    - kaarten/beheerkaart_cbs_grid.json
    """
    dataset = {"id": "beheerkaartCbsGrid", "version": "0.0.1"}
    assert (
        get_dataset_prefix_from_path(
            dataset_path="beheerkaart_cbs_grid/beheerkaart_cbs_grid.json", dataset_data=dataset
        )
        == ""
    )
    assert (
        get_dataset_prefix_from_path(
            dataset_path="kaarten/beheerkaart_cbs_grid.json", dataset_data=dataset
        )
        == "kaarten"
    )
