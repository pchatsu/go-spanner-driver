package spanner

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"log"
	"time"

	sdk "cloud.google.com/go/spanner"
    sdkpb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

var (
	ms *MockCloudSpanner
)

type spannerDriverContext struct {
}

type spannerConnector struct {
	client *sdk.Client
}

func (c *spannerConnector) Connect(context.Context) (driver.Conn, error) {
	panic("implement me")
}

func (*spannerConnector) Driver() driver.Driver {
	panic("implement me")
}

type Rows struct {
	iter            *sdk.RowIterator
	calledFirstNext bool
	isFirst         bool
	firstRow        *sdk.Row
	firstErr        error
	columns         []string
}

func (rs *Rows) Columns() []string {
	// TODO the number of column is unknown before iter.Next() are called.
	if rs.calledFirstNext {
		return rs.columns
	}

	rs.firstRow, rs.firstErr = rs.iter.Next()
	rs.isFirst = true
	rs.calledFirstNext = true

	if rs.firstRow != nil {
		rs.columns = rs.firstRow.ColumnNames()
	}
	return rs.columns
}

func (rs *Rows) Close() error {
	rs.iter.Stop()
	return nil
}

func (rs *Rows) Next(dest []driver.Value) error {
	var r *sdk.Row
	var err error

	if rs.isFirst {
		r = rs.firstRow
		err = rs.firstErr
		rs.isFirst = false
	} else {
		r, err = rs.iter.Next()
	}

	if err != nil {
		return err
	}

	for i := range dest {
		var gcv sdk.GenericColumnValue
		if err := r.Column(i, &gcv); err != nil {
			return err
		}

		// TODO 他の型もケアする
		switch gcv.Type.Code {
		case sdkpb.TypeCode_STRING:
			dest[i] = gcv.Value.GetStringValue()
		}
	}
	return nil
}

type Conn struct {
	client *sdk.Client
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// TODO data binding
	stmt := sdk.Statement{
		SQL: query,
	}

	// FIXME server mock
	go func() {
		msgs := []MockCtlMsg{
			{},
			{},
			{Err: io.EOF, ResumeToken: false},
		}

		for _, m := range msgs {
			ms.AddMsg(m.Err, m.ResumeToken)
		}
	}()

	iter := c.client.Single().Query(ctx, stmt)

	//for {
	//	row, err := iter.Next()
	//	if err == iterator.Done {
	//		break
	//	}
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//
	//	var k,v string
	//	row.Column(0, &k)
	//	row.Column(1, &v)
	//	fmt.Println(k, v)
	//}

	return &Rows{iter: iter}, nil
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	fmt.Println(query)
	panic("implement me")
}

func (c *Conn) Close() error {
	// TODO close
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	panic("implement me")
}

type Driver struct {
}

func (Driver) Open(name string) (driver.Conn, error) {
	ctx := context.Background()
	c, err := sdk.NewClient(ctx, "projects/P/instances/I/databases/D",
		option.WithEndpoint(ms.Addr()),
		option.WithGRPCDialOption(grpc.WithInsecure()),
		option.WithoutAuthentication())
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	return &Conn{client: c}, nil
}

func init() {
	trxTs := time.Unix(1, 2)
	ms = NewMockCloudSpanner(trxTs)
	ms.Serve()

	sql.Register("spanner", &Driver{})
}
