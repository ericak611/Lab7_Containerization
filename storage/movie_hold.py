from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime


class MovieHold(Base):
    """ Movie Hold """

    __tablename__ = "movie_hold"

    id = Column(Integer, primary_key=True)
    movie_id = Column(String(250), nullable=False)
    user_id = Column(String(250), nullable=False)
    branch_id = Column(Integer, nullable=False)
    availability = Column(Integer, nullable=False)
    timestamp = Column(String(100), nullable=False)
    date_created = Column(DateTime, nullable=False)
    trace_id = Column(String(250), nullable=False)


    def __init__(self, movie_id, user_id, branch_id, availability, timestamp, trace_id):
        """ Initializes a movie hold request """
        self.movie_id = movie_id
        self.user_id = user_id
        self.timestamp = timestamp
        self.branch_id = branch_id
        self.availability = availability
        self.date_created = datetime.datetime.now() # Sets the date/time record is created
        self.trace_id = trace_id

    def to_dict(self):
        """ Dictionary Representation of a movie hold request """
        dict = {}
        dict['id'] = self.id
        dict['movie_id'] = self.movie_id
        dict['user_id'] = self.user_id
        dict['timestamp'] = self.timestamp
        dict['branch_id'] = self.branch_id
        dict['availability'] = self.availability
        dict['date_created'] = self.date_created
        dict['trace_id'] = self.trace_id

        return dict
