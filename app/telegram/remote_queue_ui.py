"""Pure presentation model for the Telegram remote-queue view."""
from __future__ import annotations

from collections.abc import Callable
import re


GroupKey = Callable[[dict], tuple]
ItemLabel = Callable[[dict], str]


def build_remote_queue_groups(
    jobs: list[dict], group_key: GroupKey, item_label: ItemLabel,
) -> tuple[list[dict], list[dict]]:
    processing = [job for job in jobs if job.get('status') == 'processing']
    waiting = [job for job in jobs if job.get('status') in ('queued', 'paused')]
    groups: list[dict] = []
    indexes: dict[tuple, int] = {}

    for job in waiting:
        key = group_key(job)
        if key not in indexes:
            indexes[key] = len(groups)
            groups.append({'key': key, 'jobs': []})
        groups[indexes[key]]['jobs'].append(job)

    for group in groups:
        group_jobs = group['jobs']
        first = group_jobs[0]
        payload = first.get('payload') or {}
        titles = [
            (job.get('payload') or {}).get('manual_title') or ''
            for job in group_jobs
        ]
        episode_numbers: list[int] = []
        season_number: int | None = None
        series_title: str | None = None
        for title in titles:
            match = re.match(
                r'^(.*?)\s+S(\d{1,3})E(\d{1,4})(?:\b|\s)', title, re.I
            )
            if match:
                series_title = series_title or match.group(1).strip()
                season_number = season_number or int(match.group(2))
                episode_numbers.append(int(match.group(3)))

        batch_title = first.get('batch_title')
        if batch_title:
            label = str(batch_title).replace(' – ', ' · ')
        elif series_title is not None and season_number is not None:
            emoji = '🎌' if payload.get('is_anime') else '📺'
            label = f'{emoji} {series_title} · Staffel {season_number}'
        else:
            label = item_label(payload)

        if len(group_jobs) > 1:
            label += f' · {len(group_jobs)} Folgen'
        if episode_numbers:
            first_ep, last_ep = min(episode_numbers), max(episode_numbers)
            label += (
                f' · E{first_ep:02d}' if first_ep == last_ep
                else f' · E{first_ep:02d}–E{last_ep:02d}'
            )
        group.update({
            'anchor_id': int(first['id']),
            'label': label,
            'count': len(group_jobs),
            'paused': all(job.get('status') == 'paused' for job in group_jobs),
        })
    return processing, groups
