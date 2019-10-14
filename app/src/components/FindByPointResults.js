// @flow

import React, { createRef, Component } from 'react'
import Container from "react-bootstrap/Container";
import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";

export default class FindByPointResults extends Component {
  render() {
    return (
      <Container>
      <Row>
        <Col>
          <h3>Results</h3>

          <p>{this.props.latlng}</p>
        </Col>        
      </Row>
    </Container>
    )
  }
}