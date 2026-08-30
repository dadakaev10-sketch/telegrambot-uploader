"""Pure quality-selection helpers without network or Telegram dependencies."""
from __future__ import annotations

from dataclasses import dataclass
import json
import os
import re
import unicodedata
from urllib.parse import urljoin


@dataclass(frozen=True)
class HlsVariant:
    url: str
    height: int | None = None
    bandwidth: int = 0


def parse_hls_variants(master_url: str, playlist: str) -> list[HlsVariant]:
    variants: list[HlsVariant] = []
    stream_info: str | None = None
    for raw_line in playlist.splitlines():
        line = raw_line.strip()
        if line.startswith('#EXT-X-STREAM-INF'):
            stream_info = line
            continue
        if not line or line.startswith('#') or stream_info is None:
            continue
        resolution = re.search(r'RESOLUTION=\d+x(\d+)', stream_info, re.I)
        bandwidth = re.search(r'BANDWIDTH=(\d+)', stream_info, re.I)
        variants.append(HlsVariant(
            url=urljoin(master_url, line),
            height=int(resolution.group(1)) if resolution else None,
            bandwidth=int(bandwidth.group(1)) if bandwidth else 0,
        ))
        stream_info = None
    return variants


def choose_hls_variant(
    variants: list[HlsVariant], preferred_height: int | None = None,
) -> HlsVariant | None:
    if not variants:
        return None
    if preferred_height:
        eligible = [
            variant for variant in variants
            if variant.height is not None and variant.height <= preferred_height
        ]
        if eligible:
            return max(eligible, key=lambda variant: (variant.height or 0, variant.bandwidth))
        return min(
            variants,
            key=lambda variant: (
                variant.height if variant.height is not None else 10**9,
                variant.bandwidth,
            ),
        )
    ordered = sorted(variants, key=lambda variant: variant.bandwidth)
    return ordered[len(ordered) // 2]


DEFAULT_OVERRIDES: tuple[tuple[str, int], ...] = (
    (r'^swats0?([1-7])e\d+', 480),
)


def quality_overrides_from_env() -> tuple[tuple[str, int], ...]:
    raw = os.getenv('QUALITY_OVERRIDES_JSON', '').strip()
    if not raw:
        return DEFAULT_OVERRIDES
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError('QUALITY_OVERRIDES_JSON muss eine Liste sein')
    result: list[tuple[str, int]] = []
    for item in data:
        if not isinstance(item, dict) or not item.get('pattern') or not item.get('height'):
            raise ValueError('Jede Qualitätsregel benötigt pattern und height')
        re.compile(str(item['pattern']))
        result.append((str(item['pattern']), int(item['height'])))
    return tuple(result)


def one_time_preferred_height(title: str | None) -> int | None:
    """Select a configured source height without forcing a transcode."""
    normalized = unicodedata.normalize('NFKC', title or '').casefold()
    normalized = re.sub(r'[^a-z0-9]+', '', normalized)
    for pattern, height in quality_overrides_from_env():
        if re.search(pattern, normalized):
            return height
    return None
