from . import main


def test_calculate_sessions():
    results = main.calculate_sessions('u1', [0, 30, 60, 12000])
    expected_results = ['u1_0', 'u1_0', 'u1_0', 'u1_1']
    assert results == expected_results
