package geo
import "math"

const (
	metresPerLat = 111320.0
	metresPerLng = 97700.0
)

const (
	ZoneLat    = 28.635
	ZoneLng    = 77.225
	ZoneRadius = 2500
)

func DistanceToZone(lat, lng float64) float64 {
	return DistanceBetween(lat, lng, ZoneLat, ZoneLng)
}

func DistanceBetween(lat1, lng1, lat2, lng2 float64) float64{
	dLat := (lat1 - lat2) * metresPerLat
	dLng := (lng1 - lng2) * metresPerLng
	return math.Hypot(dLat, dLng)
}