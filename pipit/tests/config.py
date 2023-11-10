import pipit as pp


def test_get_option():
    # assert that default values are returned
    assert pp.get_option("log_level") == "INFO"
    assert pp.get_option("notebook_url") == "http://localhost:8888"

    # assert that invalid key raises ValueError
    try:
        pp.get_option("invalid_key")
    except ValueError:
        pass
    else:
        assert False


def test_set_option():
    # assert that valid values are set
    pp.set_option("log_level", "DEBUG")
    assert pp.get_option("log_level") == "DEBUG"

    pp.set_option("notebook_url", "http://127.0.0.1:8080")
    assert pp.get_option("notebook_url") == "http://127.0.0.1:8080"

    # assert that invalid key raises ValueError
    try:
        pp.set_option("invalid_key", "invalid_value")
    except ValueError:
        pass
    else:
        assert False

    # assert that invalid value raises ValueError
    try:
        pp.set_option("log_level", "invalid_value")
    except ValueError:
        pass
    else:
        assert False

    try:
        pp.set_option("notebook_url", "invalid_value")
    except ValueError:
        pass
    else:
        assert False
