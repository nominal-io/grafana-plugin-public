package plugin

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	conjurehttpclient "github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
	"github.com/palantir/conjure-go-runtime/v2/conjure-go-contract/codecs"
	"github.com/palantir/pkg/bearertoken"
	werror "github.com/palantir/witchcraft-go-error"
)

// computeClient is the generated compute client with BatchComputeWithUnits
// rebuilt so the plugin controls how the response body is decoded.
type computeClient struct {
	computeapi1.ComputeServiceClient
	client conjurehttpclient.Client
}

func newComputeClient(client conjurehttpclient.Client) computeClient {
	return computeClient{ComputeServiceClient: computeapi1.NewComputeServiceClient(client), client: client}
}

// BatchComputeWithUnits matches the generated method except for the response
// decoder. Server errors decode as generic Conjure errors, because the Nominal
// API client's typed error registry is internal to it. A generic error keeps
// its code and name even when its parameters do not fit the typed definition,
// where the generated client falls back to the bare HTTP status.
func (c computeClient) BatchComputeWithUnits(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi1.BatchComputeWithUnitsRequest) (computeapi.BatchComputeWithUnitsResponse, error) {
	var returnVal *computeapi.BatchComputeWithUnitsResponse
	if _, err := c.client.Post(ctx,
		conjurehttpclient.WithRPCMethodName("BatchComputeWithUnits"),
		conjurehttpclient.WithHeader("Authorization", fmt.Sprint("Bearer ", authHeader)),
		conjurehttpclient.WithPathf("/compute/v2/compute/batch"),
		conjurehttpclient.WithJSONRequest(requestArg),
		conjurehttpclient.WithResponseBody(&returnVal, computeResponseDecoder{codecs.JSON}),
	); err != nil {
		return computeapi.BatchComputeWithUnitsResponse{}, werror.WrapWithContextParams(ctx, err, "batchComputeWithUnits failed")
	}
	if returnVal == nil {
		return computeapi.BatchComputeWithUnitsResponse{}, werror.ErrorWithContextParams(ctx, "batchComputeWithUnits response cannot be nil")
	}
	return *returnVal, nil
}

// computeResponseDecoder decodes the result types it recognizes with
// encoding/json/v2 and leaves everything else to the generated decoder, so it
// returns the same values and errors as codecs.JSON. The generated decoder is
// slow because every nested type re-decodes its own bytes.
//
// It embeds codecs.JSON for Accept and Unmarshal; the HTTP client only calls Decode.
type computeResponseDecoder struct{ codecs.Decoder }

// Unknown members fall back rather than being dropped, so a field the fast path
// does not model can never be lost. v2 also rejects duplicate names, invalid
// UTF-8 and names that differ only in case, which v1 accepts; those fall back too.
//
// v2 would call the generated UnmarshalJSON methods, which decode with v1 again.
// The unmarshalers below replace them: each decodes into a copy of the type
// without methods, then applies the generated method's nil-to-empty rules. The
// small unions, ComputeUnitResult and PageToken, keep their generated methods.
var computeDecodeOptions = jsonv2.JoinOptions(
	jsonv2.RejectUnknownMembers(true),
	jsonv2.WithUnmarshalers(jsonv2.JoinUnmarshalers(
		jsonv2.UnmarshalFromFunc(func(dec *jsontext.Decoder, r *computeapi.BatchComputeWithUnitsResponse) error {
			type plain computeapi.BatchComputeWithUnitsResponse
			if err := jsonv2.UnmarshalDecode(dec, (*plain)(r)); err != nil {
				return err
			}
			emptyIfNil(&r.Results)
			return nil
		}),
		// A result the fast path does not handle decodes as the generated client
		// would, so one grouped or error result does not slow the rest of the batch.
		jsonv2.UnmarshalFromFunc(func(dec *jsontext.Decoder, r *computeapi.ComputeWithUnitsResult) error {
			raw, err := dec.ReadValue()
			if err != nil {
				return err
			}
			if *r, err = decodeComputeResult(raw, dec.Options()); err != nil {
				log.DefaultLogger.Debug("Compute result left to the generated decoder", "error", redactDecodeError(err))
				return codecs.JSON.Unmarshal(raw, r)
			}
			return nil
		}),
	)),
)

// emptyIfNil applies the generated decoders' rule that a missing or null list
// decodes as empty.
func emptyIfNil[T any](s *[]T) {
	if *s == nil {
		*s = []T{}
	}
}

func (computeResponseDecoder) Decode(r io.Reader, v any) error {
	body, err := io.ReadAll(r)
	if err != nil {
		// The generated decoder stops reading at the end of the first value, so
		// a read error after a complete body does not fail it.
		return codecs.JSON.Decode(io.MultiReader(bytes.NewReader(body), r), v)
	}
	if err := jsonv2.Unmarshal(body, v, computeDecodeOptions); err != nil {
		// Decoding the whole body again gives the generated decoder's error,
		// which a failed result alone can word differently. The generated
		// UnmarshalJSON replaces the whole response, so nothing v2 wrote survives.
		log.DefaultLogger.Debug("Compute response left to the generated decoder", "error", redactDecodeError(err))
		return codecs.JSON.Decode(bytes.NewReader(body), v)
	}
	return nil
}

// redactDecodeError keeps unknown-member errors, which name only a field the
// decoder does not model. Other decode errors can quote response data, such as
// a log argument's key or a malformed number.
func redactDecodeError(err error) error {
	if err == nil || errors.Is(err, jsonv2.ErrUnknownName) {
		return err
	}
	return errors.New("value did not decode")
}

// decodeComputeResult decodes one result if it is a success of a response type
// listed here, and returns an error otherwise. opts is computeDecodeOptions,
// passed in because those options call this function.
func decodeComputeResult(raw []byte, opts jsonv2.Options) (computeapi.ComputeWithUnitsResult, error) {
	var res struct {
		ComputeResult struct {
			Type    string `json:"type"`
			Success *struct {
				Type                 string                `json:"type"`
				ArrowBucketedNumeric *computeapi.ArrowPlot `json:"arrowBucketedNumeric"`
			} `json:"success"`
		} `json:"computeResult"`
		UnitResult computeapi.ComputeUnitResult `json:"unitResult"`
	}
	if err := jsonv2.Unmarshal(raw, &res, opts); err != nil {
		return computeapi.ComputeWithUnitsResult{}, err
	}
	success := res.ComputeResult.Success
	if res.ComputeResult.Type != "success" || success == nil {
		return computeapi.ComputeWithUnitsResult{}, fmt.Errorf("unhandled compute result type %q", res.ComputeResult.Type)
	}
	var resp computeapi.ComputeNodeResponse
	switch {
	case success.Type == "arrowBucketedNumeric" && success.ArrowBucketedNumeric != nil:
		resp = computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(*success.ArrowBucketedNumeric)
	default:
		return computeapi.ComputeWithUnitsResult{}, fmt.Errorf("unhandled compute response type %q", success.Type)
	}
	return computeapi.ComputeWithUnitsResult{ComputeResult: computeapi.NewComputeNodeResultFromSuccess(resp), UnitResult: res.UnitResult}, nil
}
