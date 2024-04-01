package v2a_test

import (
	"strings"
	"testing"

	"github.com/seborama/1brc/model"
	"github.com/seborama/1brc/v2a"
	"github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	src := strings.NewReader(`Salt Lake City;18.9
London;25.1
Addis Ababa;21.3
Managua;20.9
Abéché;47.1
Oranjestad;21.5
Bergen;-8.6
Ifrane;30.1
San Juan;34.7
Omaha;19.5
Bamako;31.5
Beijing;6.0
Pyongyang;16.1
Helsinki;22.3
San Salvador;-2.9
Jayapura;21.8
Pontianak;24.2
Suwałki;0.0
Phoenix;7.0
Suva;15.0
Yakutsk;-1.0
Ashgabat;-1.0
Cairns;28.2
Charlotte;22.6
Niigata;10.9
Novosibirsk;5.0
Ségou;31.3
Wichita;0.6
Heraklion;21.8
Hamilton;22.0
Frankfurt;15.9
Memphis;19.1
Lake Tekapo;20.6
Los Angeles;11.5
Kabul;6.0
Djibouti;16.5`)
	res, err := v2a.Run(src)
	require.NoError(t, err)

	expected := []*model.StationStats{
		{
			Name:  "Abéché",
			Min:   0,
			Max:   0,
			Sum:   0,
			Count: 0,
		},
	}
	require.Equal(t, expected, res)
}
