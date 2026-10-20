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
	"github.com/palantir/pkg/safelong"
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
//
// If TestComputeResponseDecoderMatchesGenerated fails after a client update,
// copy the changed nil-to-empty rules from the generated UnmarshalJSON here.
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
		jsonv2.UnmarshalFromFunc(func(dec *jsontext.Decoder, s *safelong.SafeLong) error {
			var n int64
			if err := jsonv2.UnmarshalDecode(dec, &n); err != nil {
				return err
			}
			v, err := safelong.NewSafeLong(n)
			*s = v
			return err
		}),
		jsonv2.UnmarshalFromFunc(func(dec *jsontext.Decoder, p *computeapi.NumericPlot) error {
			type plain computeapi.NumericPlot
			if err := jsonv2.UnmarshalDecode(dec, (*plain)(p)); err != nil {
				return err
			}
			emptyIfNil(&p.Timestamps)
			emptyIfNil(&p.Values)
			return nil
		}),
		jsonv2.UnmarshalFromFunc(func(dec *jsontext.Decoder, p *computeapi.PagedLogPlot) error {
			type plain computeapi.PagedLogPlot
			if err := jsonv2.UnmarshalDecode(dec, (*plain)(p)); err != nil {
				return err
			}
			emptyIfNil(&p.Timestamps)
			emptyIfNil(&p.Values)
			return nil
		}),
		jsonv2.UnmarshalFromFunc(func(dec *jsontext.Decoder, l *computeapi.LogValue) error {
			type plain computeapi.LogValue
			if err := jsonv2.UnmarshalDecode(dec, (*plain)(l)); err != nil {
				return err
			}
			if l.Args == nil {
				l.Args = map[string]string{}
			}
			return nil
		}),
		jsonv2.UnmarshalFromFunc(func(dec *jsontext.Decoder, p *computeapi.EnumPlot) error {
			type plain computeapi.EnumPlot
			if err := jsonv2.UnmarshalDecode(dec, (*plain)(p)); err != nil {
				return err
			}
			emptyIfNil(&p.Timestamps)
			emptyIfNil(&p.Values)
			emptyIfNil(&p.Categories)
			return nil
		}),
		jsonv2.UnmarshalFromFunc(func(dec *jsontext.Decoder, p *computeapi.BucketedEnumPlot) error {
			type plain computeapi.BucketedEnumPlot
			if err := jsonv2.UnmarshalDecode(dec, (*plain)(p)); err != nil {
				return err
			}
			emptyIfNil(&p.Timestamps)
			emptyIfNil(&p.Buckets)
			emptyIfNil(&p.Categories)
			return nil
		}),
		jsonv2.UnmarshalFromFunc(func(dec *jsontext.Decoder, b *computeapi.EnumBucket) error {
			type plain computeapi.EnumBucket
			if err := jsonv2.UnmarshalDecode(dec, (*plain)(b)); err != nil {
				return err
			}
			if b.Histogram == nil {
				b.Histogram = map[int]safelong.SafeLong{}
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
				Type                 string                       `json:"type"`
				ArrowBucketedNumeric *computeapi.ArrowPlot        `json:"arrowBucketedNumeric"`
				Numeric              *computeapi.NumericPlot      `json:"numeric"`
				PagedLog             *computeapi.PagedLogPlot     `json:"pagedLog"`
				Enum                 *computeapi.EnumPlot         `json:"enum"`
				BucketedEnum         *computeapi.BucketedEnumPlot `json:"bucketedEnum"`
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
	// The generated decoder keeps every arm present in the input, and the
	// constructors below set only one, so a response with two arms falls back.
	var resp computeapi.ComputeNodeResponse
	var arm string
	arms := 0
	if s := success.ArrowBucketedNumeric; s != nil {
		arms, arm, resp = arms+1, "arrowBucketedNumeric", computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(*s)
	}
	if s := success.Numeric; s != nil {
		arms, arm, resp = arms+1, "numeric", computeapi.NewComputeNodeResponseFromNumeric(*s)
	}
	if s := success.PagedLog; s != nil {
		arms, arm, resp = arms+1, "pagedLog", computeapi.NewComputeNodeResponseFromPagedLog(*s)
	}
	if s := success.Enum; s != nil {
		arms, arm, resp = arms+1, "enum", computeapi.NewComputeNodeResponseFromEnum(*s)
	}
	if s := success.BucketedEnum; s != nil {
		arms, arm, resp = arms+1, "bucketedEnum", computeapi.NewComputeNodeResponseFromBucketedEnum(*s)
	}
	if arms != 1 || arm != success.Type {
		return computeapi.ComputeWithUnitsResult{}, fmt.Errorf("compute response type %q with %d arms", success.Type, arms)
	}
	return computeapi.ComputeWithUnitsResult{ComputeResult: computeapi.NewComputeNodeResultFromSuccess(resp), UnitResult: res.UnitResult}, nil
}
