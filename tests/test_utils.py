"""Test utility functions."""

import pytest
from httpx import RequestError

from citations.utils import (
    generate_unique_id,
    get_with_waiting,
    normalize_title,
)


def test_get_with_waiting(httpx_mock):
    response_text = "Great response"
    url = "https://dummy.com"
    httpx_mock.add_response(url=url, method="GET", text=response_text)
    response = get_with_waiting(url)
    assert response.text == response_text


def test_get_with_waiting_retry(httpx_mock):
    response_text = "Great response"
    url = "https://dummy.com"
    httpx_mock.add_exception(RequestError("Request failed"), url=url)
    httpx_mock.add_response(url=url, method="GET", text=response_text)
    response = get_with_waiting(url, wait=0.01)
    assert response.text == response_text


def test_generate_unique_id_different_input():
    name1 = "Institution One"
    name2 = "Institution Two"
    id1 = generate_unique_id(name1)
    id2 = generate_unique_id(name2)
    assert id1 != id2


@pytest.mark.parametrize(
    "input1, input2",
    [
        (
            (
                "From Big Data to Big Displays High-Performance Visualization"
                " at Blue Brain"
            ),
            (
                "From Big Data To big Displays High-Performance visualization"
                " at Blue Brain"
            ),
        ),
        (
            "The Scientific Case for Brain Simulations",
            "The Scientific Case for Brain Simulations.",
        ),
        (
            "  Neurobiological Causal Models of Language Processing   ",
            "Neurobiological Causal Models of Language Processing.",
        ),
    ],
)
def test_normalize_bbp_title(input1, input2):
    assert normalize_title(input1) == normalize_title(input2)
