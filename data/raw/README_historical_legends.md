# Historical Legends Dataset (Pre-1974)

## Overview
Curated dataset of undisputed NFL legends from the pre-1974 era to supplement nflverse data coverage. These players represent the most iconic figures from NFL's early history.

## Data Source
- **Primary**: Pro Football Hall of Fame charter class (1963) + early inductees
- **Schema**: Matches `players_index_full.csv` exactly for seamless integration
- **Coverage**: 1915-1973 playing careers

## Player Selection Criteria
- **Hall of Fame Status**: All players are HOF inductees
- **Historical Significance**: Charter class members + undisputed legends
- **Era Coverage**: Spans from Jim Thorpe (1915) to early 1970s

## Schema Notes

### Available Fields
- **Identity**: synthetic `player_id` (HOF-YEAR-###), full names, positions
- **Career Span**: first_year, last_year, estimated career_seasons and games
- **Stats**: Career passing/rushing/receiving yards and TDs where known
- **Physical**: Era-appropriate height/weight estimates
- **Honors**: All marked as HOF, Pro Bowl/All-Pro data missing (pre-1950 mostly)

### Missing/Estimated Fields
- **Birth dates**: Best estimates, some approximate
- **College**: Known for most players
- **Combine data**: Not available (pre-combine era)
- **Playoff stats**: Championships existed but detailed playoff stats unavailable
- **Defensive stats**: Limited availability for early era

### Data Quality Tiers
- **Tier 1 (High confidence)**: Names, positions, career spans, HOF status
- **Tier 2 (Good estimates)**: Height/weight, major career stats for skill positions
- **Tier 3 (Placeholders)**: Draft picks (999 = pre-draft), combine metrics (null)

## Integration Strategy
1. Load alongside `players_index_full.csv` in pipeline
2. Use `player_id` prefix `HOF-` to distinguish historical vs nflverse data
3. Apply same FRCS attribute mapping with era-appropriate adjustments
4. Mark data provenance as "historical_curated" vs "nflverse"

## Future Expansion
- Can add more pre-1974 legends (target ~50-100 total)
- Consider adding estimated All-Pro selections for major stars
- Potential integration with Pro Football Reference historical data

## Notable Players Included
- **Pioneers**: Jim Thorpe, Red Grange, Bronko Nagurski
- **Golden Age**: Sammy Baugh, Don Hutson, Otto Graham
- **Modern Legends**: Jim Brown, Johnny Unitas, Joe Namath
- **Two-Way Greats**: Chuck Bednarik, Marion Motley

Last Updated: 2025-08-23