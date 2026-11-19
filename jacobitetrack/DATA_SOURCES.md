# JacobiteTrack — next crossing and engineering works feeds

The live Darwin/VSTP collector retains scheduled Glenfinnan station times for up to seven days and merges them with actual movement reports. Its output is `jacobitetrack/feed.json`.

## Published timetable

Optional GitHub Actions secrets:
- `RAIL_FUTURE_TIMETABLE_URL`: HTTPS URL for an authorised future-dated timetable feed adapted to the format below
- `RAIL_FUTURE_TIMETABLE_TOKEN`: optional Bearer token

The endpoint must return:
```json
{
  "crossings": [
    {"id": "service-id", "at": "2026-11-20T10:35:00Z", "direction": "fort_william"}
  ]
}
```

`at` is the **estimated Glenfinnan Viaduct crossing time**, not the Glenfinnan station time. Valid directions are `fort_william` and `mallaig`. Use a timetable with service dates and cancellation coverage. No crossing will be invented when the source is unavailable; today's services are not blindly repeated on future days.

## Planned engineering works

Optional GitHub Actions secrets:
- `RAIL_PLANNED_WORKS_URL`: HTTPS URL for authorised, relevant Mallaig–Fort William engineering works notices, adapted to the format below
- `RAIL_PLANNED_WORKS_TOKEN`: optional Bearer token

The endpoint must return a JSON object containing `works`:
```json
{
  "works": [
    {
      "id": "notice-123",
      "title": "Example — planned closure",
      "start": "2026-11-20T00:00:00Z",
      "end": "2026-11-21T23:59:00Z",
      "stations": ["Glenfinnan", "Mallaig"],
      "url": "https://example.org/notice"
    }
  ]
}
```

The example is illustrative, not an actual engineering works notice. Provide only notices affecting the monitored line. The app shows a compact warning for relevant notices in the next seven days. If there is no configured source or the last successful check is older than a day, it displays **Engineering works: data unavailable**, rather than incorrectly claiming the line is clear.

## Live feed and confidence

Train positions and crossing estimates are based on rail timetable and station-time reports, not GPS on the train. The UI prioritises a current crossing estimate over a later scheduled prediction when appropriate. Future-dated timetables do not mean that a train is confirmed to be operating.

The GitHub Actions workflow is `.github/workflows/darwin-feed.yml`. Credentials belong in repository Actions secrets, never in browser JavaScript or source code.
