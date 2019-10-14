// @flow

import React, { createRef, useState, Component } from 'react'
import Container from "react-bootstrap/Container";
import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";

import SimpleLeaflet from "./SimpleLeaflet"
import FindByPointResults from "./FindByPointResults"


export default class FindByPointComponent extends Component<{}, State> {
  constructor(props) {
    super(props)

    this.state = {
      latlng: null
    }
    this.updateResult = this.updateResult.bind(this);


    this.testFn = (latlng) => {
      console.log("TestFN here! " + latlng);
      this.updateResult(latlng)      
    };
  }
  renderlatlng(latlng) {
    return latlng.lat + ", " + latlng.lng;
  }

  updateResult(latlng) {
    // Explicitly focus the text input using the raw DOM API
    // Note: we're accessing "current" to get the DOM node
    this.setState({
      latlng: latlng
    });
    console.log("State updated!")
    console.log(this.state)
  }

  render() {
    var ll = null;
    if (this.state.latlng) {
       ll = this.state.latlng.lat.toString() + ", " + this.state.latlng.lng.toString() ;
    }
    return (
      <Container fluid='true'>
        <Row>
          <Col sm={8}>
            <SimpleLeaflet inputRef={this.testFn} />
          </Col>
          <Col sm={4}>
            <FindByPointResults latlng={ll} />
          </Col>
        </Row>
      </Container>
    )
  }
}