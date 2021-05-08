using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using M220N.Models;
using M220N.Models.Projections;
using M220N.Models.Responses;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;

namespace M220N.Repositories
{
    public class CommentsRepository
    {
        private readonly IMongoCollection<Comment> _commentsCollection;
        private readonly MoviesRepository _moviesRepository;

        public CommentsRepository(IMongoClient mongoClient)
        {
            var camelCaseConvention = new ConventionPack {new CamelCaseElementNameConvention()};
            ConventionRegistry.Register("CamelCase", camelCaseConvention, type => true);

            _commentsCollection = mongoClient.GetDatabase("sample_mflix").GetCollection<Comment>("comments");
            _moviesRepository = new MoviesRepository(mongoClient);
        }

        /// <summary>
        ///     Adds a comment.
        /// </summary>
        /// <param name="user"></param>
        /// <param name="movieId"></param>
        /// <param name="comment"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>The Movie associated with the comment.</returns>
        public async Task<Movie> AddCommentAsync(User user, ObjectId movieId, string comment,
            CancellationToken cancellationToken = default)
        {
            try
            {
                var newComment = new Comment
                {
                    Date = DateTime.UtcNow,
                    Text = comment,
                    Name = user.Name,
                    Email = user.Email,
                    MovieId = movieId
                };

                await _commentsCollection.InsertOneAsync(newComment, cancellationToken: cancellationToken); 

                // Ticket: Add a new Comment
                // Implement InsertOneAsync() to insert a
                // new comment into the comments collection.

                return await _moviesRepository.GetMovieAsync(movieId.ToString(), cancellationToken);
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        ///     Updates an existing comment. Only the comment owner can update the comment.
        /// </summary>
        /// <param name="user"></param>
        /// <param name="movieId"></param>
        /// <param name="commentId"></param>
        /// <param name="comment"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>An UpdateResult</returns>
        public async Task<UpdateResult> UpdateCommentAsync(User user,
            ObjectId movieId, ObjectId commentId, string comment,
            CancellationToken cancellationToken = default)
        {
            // Ticket: Update a Comment
            // Implement UpdateOneAsync() to update an
            // existing comment. Remember that only the original
            // comment owner can update the comment!
            //
            return await _commentsCollection.UpdateOneAsync(
            Builders<Comment>.Filter.Where(x=>x.Id == commentId && x.MovieId == movieId && x.Email == user.Email),
            Builders<Comment>.Update.Set(c=>c.Text,comment).Set(co=>co.Date,DateTime.UtcNow),
            new UpdateOptions { IsUpsert = false},
            cancellationToken);
        }

        /// <summary>
        ///     Deletes a comment. Only the comment owner can delete a comment.
        /// </summary>
        /// <param name="movieId"></param>
        /// <param name="commentId"></param>
        /// <param name="user"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>The movie associated with the comment that is being deleted.</returns>
        public async Task<Movie> DeleteCommentAsync(ObjectId movieId, ObjectId commentId,
            User user, CancellationToken cancellationToken = default)
        {
            // Ticket: Delete a Comment
            // Implement DeleteOne() to delete an
            // existing comment. Remember that only the original
            // comment owner can delete the comment!
            await _commentsCollection.DeleteOneAsync(
                Builders<Comment>.Filter.Where(
                    c => c.MovieId == movieId
                         && c.Id == commentId && c.Email == user.Email),cancellationToken);

            return await _moviesRepository.GetMovieAsync(movieId.ToString(), cancellationToken);
        }

        public async Task<TopCommentsProjection> MostActiveCommentersAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                List<ReportProjection> result = null;

                var projectionFilter = Builders<ReportProjection>.Projection
                    .Include(m => m.Id)
                    .Include(m => m.Count); 
                // TODO Ticket: User Report
                // Return the 20 users who have commented the most on MFlix. You will need to use
                // the Group, Sort, Limit, and Project methods of the Aggregation pipeline.
                //
                var projectionDefinition = new BsonDocument
                {
                    {"_id", "$email"},
                    {"count", new BsonDocument("$sum", 1)}
                };
                result = await _commentsCollection
                    .WithReadConcern(ReadConcern.Majority)
                    .Aggregate()
                    .Group(projectionDefinition)
                    .Sort(new BsonDocument("count", -1)).Limit(20).Project<ReportProjection>(new BsonDocument()
                    {
                        { "_Id", "$id"},
                        { "count" , "$count" }
                    })
                        .ToListAsync(cancellationToken: cancellationToken); 

                return new TopCommentsProjection(result);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw;
            }
        }
    }
}
