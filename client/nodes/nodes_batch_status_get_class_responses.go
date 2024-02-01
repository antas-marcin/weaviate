//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Code generated by go-swagger; DO NOT EDIT.

package nodes

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/entities/models"
)

// NodesBatchStatusGetClassReader is a Reader for the NodesBatchStatusGetClass structure.
type NodesBatchStatusGetClassReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *NodesBatchStatusGetClassReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewNodesBatchStatusGetClassOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewNodesBatchStatusGetClassUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewNodesBatchStatusGetClassForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewNodesBatchStatusGetClassInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewNodesBatchStatusGetClassOK creates a NodesBatchStatusGetClassOK with default headers values
func NewNodesBatchStatusGetClassOK() *NodesBatchStatusGetClassOK {
	return &NodesBatchStatusGetClassOK{}
}

/*
NodesBatchStatusGetClassOK describes a response with status code 200, with default header values.

Nodes batch status successfully returned
*/
type NodesBatchStatusGetClassOK struct {
	Payload *models.BatchStats
}

// IsSuccess returns true when this nodes batch status get class o k response has a 2xx status code
func (o *NodesBatchStatusGetClassOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this nodes batch status get class o k response has a 3xx status code
func (o *NodesBatchStatusGetClassOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this nodes batch status get class o k response has a 4xx status code
func (o *NodesBatchStatusGetClassOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this nodes batch status get class o k response has a 5xx status code
func (o *NodesBatchStatusGetClassOK) IsServerError() bool {
	return false
}

// IsCode returns true when this nodes batch status get class o k response a status code equal to that given
func (o *NodesBatchStatusGetClassOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the nodes batch status get class o k response
func (o *NodesBatchStatusGetClassOK) Code() int {
	return 200
}

func (o *NodesBatchStatusGetClassOK) Error() string {
	return fmt.Sprintf("[GET /nodes/_batch_status][%d] nodesBatchStatusGetClassOK  %+v", 200, o.Payload)
}

func (o *NodesBatchStatusGetClassOK) String() string {
	return fmt.Sprintf("[GET /nodes/_batch_status][%d] nodesBatchStatusGetClassOK  %+v", 200, o.Payload)
}

func (o *NodesBatchStatusGetClassOK) GetPayload() *models.BatchStats {
	return o.Payload
}

func (o *NodesBatchStatusGetClassOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.BatchStats)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewNodesBatchStatusGetClassUnauthorized creates a NodesBatchStatusGetClassUnauthorized with default headers values
func NewNodesBatchStatusGetClassUnauthorized() *NodesBatchStatusGetClassUnauthorized {
	return &NodesBatchStatusGetClassUnauthorized{}
}

/*
NodesBatchStatusGetClassUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type NodesBatchStatusGetClassUnauthorized struct {
}

// IsSuccess returns true when this nodes batch status get class unauthorized response has a 2xx status code
func (o *NodesBatchStatusGetClassUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this nodes batch status get class unauthorized response has a 3xx status code
func (o *NodesBatchStatusGetClassUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this nodes batch status get class unauthorized response has a 4xx status code
func (o *NodesBatchStatusGetClassUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this nodes batch status get class unauthorized response has a 5xx status code
func (o *NodesBatchStatusGetClassUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this nodes batch status get class unauthorized response a status code equal to that given
func (o *NodesBatchStatusGetClassUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the nodes batch status get class unauthorized response
func (o *NodesBatchStatusGetClassUnauthorized) Code() int {
	return 401
}

func (o *NodesBatchStatusGetClassUnauthorized) Error() string {
	return fmt.Sprintf("[GET /nodes/_batch_status][%d] nodesBatchStatusGetClassUnauthorized ", 401)
}

func (o *NodesBatchStatusGetClassUnauthorized) String() string {
	return fmt.Sprintf("[GET /nodes/_batch_status][%d] nodesBatchStatusGetClassUnauthorized ", 401)
}

func (o *NodesBatchStatusGetClassUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewNodesBatchStatusGetClassForbidden creates a NodesBatchStatusGetClassForbidden with default headers values
func NewNodesBatchStatusGetClassForbidden() *NodesBatchStatusGetClassForbidden {
	return &NodesBatchStatusGetClassForbidden{}
}

/*
NodesBatchStatusGetClassForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type NodesBatchStatusGetClassForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this nodes batch status get class forbidden response has a 2xx status code
func (o *NodesBatchStatusGetClassForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this nodes batch status get class forbidden response has a 3xx status code
func (o *NodesBatchStatusGetClassForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this nodes batch status get class forbidden response has a 4xx status code
func (o *NodesBatchStatusGetClassForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this nodes batch status get class forbidden response has a 5xx status code
func (o *NodesBatchStatusGetClassForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this nodes batch status get class forbidden response a status code equal to that given
func (o *NodesBatchStatusGetClassForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the nodes batch status get class forbidden response
func (o *NodesBatchStatusGetClassForbidden) Code() int {
	return 403
}

func (o *NodesBatchStatusGetClassForbidden) Error() string {
	return fmt.Sprintf("[GET /nodes/_batch_status][%d] nodesBatchStatusGetClassForbidden  %+v", 403, o.Payload)
}

func (o *NodesBatchStatusGetClassForbidden) String() string {
	return fmt.Sprintf("[GET /nodes/_batch_status][%d] nodesBatchStatusGetClassForbidden  %+v", 403, o.Payload)
}

func (o *NodesBatchStatusGetClassForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *NodesBatchStatusGetClassForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewNodesBatchStatusGetClassInternalServerError creates a NodesBatchStatusGetClassInternalServerError with default headers values
func NewNodesBatchStatusGetClassInternalServerError() *NodesBatchStatusGetClassInternalServerError {
	return &NodesBatchStatusGetClassInternalServerError{}
}

/*
NodesBatchStatusGetClassInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type NodesBatchStatusGetClassInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this nodes batch status get class internal server error response has a 2xx status code
func (o *NodesBatchStatusGetClassInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this nodes batch status get class internal server error response has a 3xx status code
func (o *NodesBatchStatusGetClassInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this nodes batch status get class internal server error response has a 4xx status code
func (o *NodesBatchStatusGetClassInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this nodes batch status get class internal server error response has a 5xx status code
func (o *NodesBatchStatusGetClassInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this nodes batch status get class internal server error response a status code equal to that given
func (o *NodesBatchStatusGetClassInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the nodes batch status get class internal server error response
func (o *NodesBatchStatusGetClassInternalServerError) Code() int {
	return 500
}

func (o *NodesBatchStatusGetClassInternalServerError) Error() string {
	return fmt.Sprintf("[GET /nodes/_batch_status][%d] nodesBatchStatusGetClassInternalServerError  %+v", 500, o.Payload)
}

func (o *NodesBatchStatusGetClassInternalServerError) String() string {
	return fmt.Sprintf("[GET /nodes/_batch_status][%d] nodesBatchStatusGetClassInternalServerError  %+v", 500, o.Payload)
}

func (o *NodesBatchStatusGetClassInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *NodesBatchStatusGetClassInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}
