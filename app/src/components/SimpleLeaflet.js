// @flow

import React, { createRef, Component } from 'react'
import { Map, TileLayer, Marker, Popup } from 'react-leaflet'

type State = {
  lat: number,
  lng: number,
  zoom: number,
}

export default class SimpleLeaflet extends Component<{}, State> {

  constructor(props) {
    super(props);
    
    this.state = {
      hasLocation: false,
      latlng: {
        lat: -25.2744,
        lng: 133.7751      
      },
      zoom: 5,
      inputRefFn: props.inputRef
    };    
  }


  mapRef = createRef<Map>()

  handleClick = (e: Object) => {
    const map = this.mapRef.current
    console.log("click");
    console.log(e);
    if (map != null) {
      //map.leafletElement.locate()      
      this.setState({
        latlng: e.latlng,
        hasLocation: true
      });
    }
    console.log(this.state);
    this.state.inputRefFn(this.state.latlng);
    
  }

  handleLocationFound = (e: Object) => {
    this.setState({
      hasLocation: true,
      latlng: e.latlng,
    })
  }

  render() {
    const marker = this.state.hasLocation ? (
      <Marker position={this.state.latlng}>
        <Popup>You clicked here</Popup>
      </Marker>
    ) : null

    return (
      <Map
        center={this.state.latlng}
        length={4}
        onClick={this.handleClick}
        //onLocationfound={this.handleLocationFound}
        ref={this.mapRef}
        zoom={this.state.zoom}>
        <TileLayer
          attribution='&amp;copy <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {marker}
      </Map>
    )
  }
}