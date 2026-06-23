"""单元测试:哈希算法的稳定性与抗碰撞性。

对应设计文档第 5.2 节。
"""

from tail.hashing import HASH_HEX_LENGTH, compute_prefix_hash


def test_hash_is_stable():
    """同一序列多次计算结果一致。"""
    seq = [1, 2, 3, 4, 5]
    assert compute_prefix_hash(seq) == compute_prefix_hash(seq)


def test_hash_length_is_16():
    """取 SHA256 前 16 个十六进制字符(64 bit)。"""
    h = compute_prefix_hash([1, 2, 3])
    assert len(h) == HASH_HEX_LENGTH == 16
    int(h, 16)  # 必须是合法十六进制


def test_hash_input_order_matters():
    assert compute_prefix_hash([1, 2]) != compute_prefix_hash([2, 1])


def test_hash_no_boundary_collision():
    """定长编码避免相邻 token 边界歧义:[1,2] != [12]。"""
    assert compute_prefix_hash([1, 2]) != compute_prefix_hash([12])
    assert compute_prefix_hash([1, 23]) != compute_prefix_hash([12, 3])


def test_prefix_extends_changes_hash():
    """前缀增长,哈希必须变化(协议:每次按新前缀生成新哈希)。"""
    base = compute_prefix_hash([1, 2, 3])
    extended = compute_prefix_hash([1, 2, 3, 4])
    assert base != extended


def test_empty_sequence():
    """空序列也应产出合法哈希(首次空前缀场景)。"""
    h = compute_prefix_hash([])
    assert len(h) == 16


def test_large_token_ids():
    """大 token id(>32bit)也能正确编码,不溢出。"""
    h = compute_prefix_hash([2**40, 2**40 + 1])
    assert len(h) == 16
    assert h != compute_prefix_hash([0, 1])
