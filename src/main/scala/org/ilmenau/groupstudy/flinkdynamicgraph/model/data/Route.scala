package org.ilmenau.groupstudy.flinkdynamicgraph.model.data

/**
    Airline			2-letter (IATA) or 3-letter (ICAO) code of the airline.
    Airline ID			Unique OpenFlights identifier for airline.
    Source airport		3-letter (IATA) or 4-letter (ICAO) code of the source airport.
    Source airport ID		Unique OpenFlights identifier for source airport
    Destination airport		3-letter (IATA) or 4-letter (ICAO) code of the destination airport.
    Destination airport ID	Unique OpenFlights identifier for destination airport
    Codeshare	"		Y" if this flight is a codeshare (that is, not operated by Airline, but another carrier), empty otherwise.
    Stops				Number of stops on this flight ("0" for direct)
    Equipment			3-letter codes for plane type(s) generally used on this flight, separated by spaces
  */
case class Route(airline: String,
                 airlineID: Integer,
                 sourceAirport: String,
                 sourceAirportID: Integer,
                 destAirport: String,
                 destAirportID: Integer,
                 codeshare: String,
                 stops: Integer,
                 equipment: String)