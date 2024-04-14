## Incomplete, no location
```
([a-zA-Z]\.[a-zA-Z\'\-]+) pass incomplete intended for( [a-zA-Z].[a-zA-Z\'\-]+)?.
```

## Incomplete, with location
```
([a-zA-Z]\.[a-zA-Z\'\-]+) pass incomplete ([a-zA-Z]+) ([a-zA-Z]+)? intended for( [a-zA-Z].[a-zA-Z\'\-]+)?.
```

## Complete, with location
```
([a-zA-Z]\.[a-zA-Z\'\-]+) pass ([a-zA-Z]+) ([a-zA-Z]+) complete to ([a-zA-Z]+\s[0-9]+|[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+). Catch made by ([a-zA-Z]\.[a-zA-Z\'\-]+) at ([a-zA-Z]+\s[0-9]+|[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+). Gain of( -?[0-9]+)? yards
```

## Complete, no location
```
([a-zA-Z]\.[a-zA-Z\']+) pass complete to ([a-zA-Z]+\s[0-9]+|[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+). Catch made by ([a-zA-Z]\.[a-zA-Z\']+) at ([a-zA-Z]+\s[0-9]+|[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+). Gain of( -?[0-9]+)? yards
```

## Tackle
```
Tackled by ([a-zA-Z\.\'\-\s\;]+)? at ([a-zA-Z]+\s[0-9]+|[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)
```

## Tackle (push out)
```
Pushed out of bounds by ([a-zA-Z\.\'\-\s\;]+)? at ([a-zA-Z]+\s[0-9]+|[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)
```

## Punter
```
([a-zA-Z]\.[a-zA-Z\'\-]+) punts ([0-9\-]+) yards to ([a-zA-Z]+\s[0-9]+|[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)
```