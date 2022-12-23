package discuss

import (
	"context"
	"database/sql"
	"log"
	"strconv"

	"github.com/CSC354/discuss/pdiscuss"
	"github.com/CSC354/discuss/perrors"
	"github.com/CSC354/sijl/pwathiq"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Discuss struct {
	*sql.DB
	pwathiq.WathiqClient
	pdiscuss.UnimplementedDiscussServer
}

// NewArgument implements pdiscuss.DiscussServer
func (d Discuss) NewArgument(ctx context.Context, in *pdiscuss.NewArgumentRequest) (*pdiscuss.NewArgumentResponse, error) {
	res := &pdiscuss.NewArgumentResponse{}
	v, err := d.WathiqClient.Validate(ctx, &pwathiq.ValidateRequest{Token: in.Token})
	if err != nil {
		log.Fatal(err)
	}
	if !v.Valid {
		res.Error = int32(perrors.Errors_Unauthorized)
		return res, err
	}
	if len(in.Argument) == 0 {
		res.Error = int32(perrors.Errors_InvalidArgument)
		return res, err
	}

	if in.ArgumentEnd == nil && in.ArgumentStart == nil {
		res.ID = addargument(in, &d)
	} else {
		res.ID = addresponse(in, &d)
	}
	stmt, err := d.DB.Prepare(`INSERT INTO DISCUSS.ARGUMENTS_TAGS(argument_id, tag_id) VALUES (@id, @tag)`)
	if err != nil {
		log.Fatal(err)
	}
	for _, value := range in.Tags {
		_, err = stmt.Exec(sql.Named("id", res.ID), sql.Named("tag", value))
		if err != nil {
			log.Fatal(err)
		}
	}
	res.Error = int32(perrors.Errors_Ok)
	return res, err
}

// ReadArgument implements pdiscuss.DiscussServer
func (d Discuss) ReadArgument(ctx context.Context, in *pdiscuss.ReadArgumentRequest) (*pdiscuss.ReadArgumentResponse, error) {
	arg := pdiscuss.ReadArgumentResponse{}
	stmt, err := d.DB.Prepare(`SELECT sijl_id, argument from DISCUSS.ARGUMENTS arg WHERE id == @id`)
	if err != nil {
		log.Fatal(err)
	}
	err = stmt.QueryRow(sql.Named("id", in.ID)).Scan(&arg.AuthorID, &arg.Text)
	if err != nil {
		log.Fatal(err)
	}
	stmt.Close()
	stmt, err = d.Prepare(`SELECT tag_id FROM DISCUSS.ARGUMENTS_TAGS tags WHERE @argument = tags.argument_id`)
	if err != nil {
		log.Fatal(err)
	}
	rows, err := stmt.Query(sql.Named("argument", in.ID))
	defer rows.Close()
	for rows.Next() {
		var tag int32
		err = rows.Scan(tag)
		arg.TagIDs = append(arg.TagIDs, tag)
		if err != nil {
			log.Fatal(err)
		}
	}
	stmt.Close()
	stmt, err = d.Prepare(`SELECT COUNT(*) FROM DISCUSS.VOTES votes WHERE votes.argument_id == @id`)
	if err != nil {
		log.Fatal(err)
	}
	err = stmt.QueryRow(sql.Named("id", in.ID)).Scan(arg.Votes)
	if err != nil {
		log.Fatal(err)
	}
	return &arg, err
}

// ReadLatestArguments implements pdiscuss.DiscussServer
func (d Discuss) ReadLatestArguments(context.Context, *emptypb.Empty) (*pdiscuss.Responses, error) {
	res := pdiscuss.Responses{}
	stmt, err := d.DB.Query(`SELECT id FROM DISCUSS.ARGUMENTS arg WHERE arg.in_response IS NULL ORDER BY id DESC`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	for stmt.Next() {
		var id int32
		err = stmt.Scan(&id)
		if err != nil {
			log.Fatal(err)
		}
		res.ReponsesIDs = append(res.ReponsesIDs, id)
	}
	return &res, err
}

// ReadLatestResponses implements pdiscuss.DiscussServer
func (d Discuss) ReadLatestResponses(context.Context, *emptypb.Empty) (*pdiscuss.Responses, error) {
	res := pdiscuss.Responses{}
	stmt, err := d.DB.Query(`SELECT id FROM DISCUSS.ARGUMENTS arg WHERE arg.in_response IS NOT NULL ORDER BY id DESC`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	for stmt.Next() {
		var id int32
		err = stmt.Scan(&id)
		if err != nil {
			log.Fatal(err)
		}
		res.ReponsesIDs = append(res.ReponsesIDs, id)
	}
	return &res, err
}

// ReadResponses implements pdiscuss.DiscussServer
func (d Discuss) ReadResponses(ctx context.Context, in *pdiscuss.ReadResponsesRequest) (*pdiscuss.Responses, error) {
	res := pdiscuss.Responses{}

	s, err := d.DB.Prepare(`SELECT id FROM DISCUSS.ARGUMENTS arg WHERE arg.in_response = @id ORDER BY id DESC `)
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := s.Query(sql.Named("id", in.ArgumentID))
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	for stmt.Next() {
		var id int32
		err = stmt.Scan(&id)
		if err != nil {
			log.Fatal(err)
		}
		res.ReponsesIDs = append(res.ReponsesIDs, id)
	}
	return &res, err
}

// ReadUserArguments implements pdiscuss.DiscussServer
func (d Discuss) ReadUserArguments(ctx context.Context, in *pdiscuss.ReadUserArgumentsRequest) (*pdiscuss.Responses, error) {
	res := pdiscuss.Responses{}

	s, err := d.DB.Prepare(`SELECT id FROM DISCUSS.ARGUMENTS arg WHERE arg.sijl_id = @id ORDER BY id DESC`)
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := s.Query(sql.Named("id", in.Username))
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	for stmt.Next() {
		var id int32
		err = stmt.Scan(&id)
		if err != nil {
			log.Fatal(err)
		}
		res.ReponsesIDs = append(res.ReponsesIDs, id)
	}
	return &res, err
}

// mustEmbedUnimplementedDiscussServer implements pdiscuss.DiscussServer
func (Discuss) mustEmbedUnimplementedDiscussServer() {
	panic("unimplemented")
}

// AddTag implements pdiscuss.DiscussServer
func (Discuss) AddTag(ctx context.Context, tag *pdiscuss.Tag) (*pdiscuss.Ok, error) {
	panic("unimplemented")

}

func (d Discuss) ReadTag(ctx context.Context, in *pdiscuss.ReadTagRequest) (*pdiscuss.Tag, error) {
	tag := &pdiscuss.Tag{}
	stmt, err := d.DB.Prepare(`SELECT tag_name from DISCUSS.TAGS WHERE id == @id`)
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.QueryRow(sql.Named("id", in.ID)).Scan(&tag.Tag)
	if err != nil {
		log.Fatal(err)
	}

	return tag, err
}

func addresponse(in *pdiscuss.NewArgumentRequest, d *Discuss) int32 {
	stmt, err := d.DB.Prepare(`
INSERT INTO DISCUSS.ARGUMENTS (in_response, argument, argument_start, argument_end, sijl_id)
VALUES (@in_response, @argument, @start, @end, @id)
`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	sijlID, err := d.WathiqClient.Validate(context.Background(), &pwathiq.ValidateRequest{Token: in.Token})
	if err != nil {
		log.Fatal(err)
	}
	sijlId, _ := strconv.Atoi(sijlID.Id)
	row, err := stmt.Exec(sql.Named("in_response", in.ResponseTo), sql.Named("argument", in.Argument),
		sql.Named("start", in.ArgumentStart), sql.Named("end", in.ArgumentEnd), sql.Named("id", sijlId))
	if err != nil {
		log.Fatal(err)
	}
	id, err := row.LastInsertId()
	if err != nil {
		log.Fatal(err)
	}
	return int32(id)
}

func addargument(in *pdiscuss.NewArgumentRequest, d *Discuss) int32 {
	stmt, err := d.DB.Prepare(`
INSERT INTO DISCUSS.ARGUMENTS (sijl_id, argument) VALUES (@id, @text)
`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	sijlID, err := d.WathiqClient.Validate(context.Background(), &pwathiq.ValidateRequest{Token: in.Token})
	if err != nil {
		log.Fatal(err)
	}
	sijlId, _ := strconv.Atoi(sijlID.Id)
	row, err := stmt.Exec(sql.Named("id", sijlId), sql.Named("text", in.Argument))
	if err != nil {
		log.Fatal(err)
	}
	id, err := row.LastInsertId()
	if err != nil {
		log.Fatal(err)
	}
	return int32(id)
}
