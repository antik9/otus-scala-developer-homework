package ru.otus.jdbc.dao.slick

import ru.otus.jdbc.model.{Role, User}
import slick.jdbc.PostgresProfile.api._

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class UserDaoSlickImpl(db: Database)(implicit ec: ExecutionContext) {

  import UserDaoSlickImpl._

  def getUser(userId: UUID): Future[Option[User]] = {
    val res = for {
      user <- users.filter(user => user.id === userId).result.headOption
      roles <- usersToRoles.filter(_.usersId === userId).map(_.rolesCode).result.map(_.toSet)
    } yield user.map(_.toUser(roles))

    db.run(res)
  }

  private[this] def createUserInDb(user: User): Future[User] = {
    val action = for {
      id <- users returning users.map(_.id) += UserRow.fromUser(user)
      newUser = user.copy(id = Some(id))
      _ <- usersToRoles ++= newUser.roles.map(id -> _)
    } yield newUser

    db.run(action.transactionally)
  }

  def createUser(user: User): Future[User] =
    user.id match {
      case Some(id) => getUser(id).flatMap {
        case Some(user) => Future {
          user
        }
        case None => createUserInDb(user)
      }
      case None => createUserInDb(user)
    }

  def updateUser(user: User): Future[Unit] = {
    user.id match {
      case Some(userId) => {
        val updateUser = for {
          dbUser <- users.forUpdate.filter(_.id === userId).result.headOption
          _ <- {
            dbUser match {
              case Some(_) => users
                .filter(_.id === userId)
                .map(u => (u.firstName, u.lastName, u.age))
                .update((user.firstName, user.lastName, user.age))
              case None => users += UserRow.fromUser(user)
            }
          }
        } yield ()

        val deleteRoles = usersToRoles.filter(_.usersId === userId).delete
        val insertRoles = usersToRoles ++= user.roles.map(userId -> _)

        val action = updateUser >> deleteRoles >> insertRoles >> DBIO.successful(())
        db.run(action.transactionally)
      }
      case None => Future.successful(())
    }
  }

  def deleteUser(userId: UUID): Future[Option[User]] = {
    getUser(userId).flatMap {
      case Some(user) => {
        val deleteRoles = usersToRoles.filter(_.usersId === userId).delete
        val deleteUser = users.filter(_.id === userId).delete
        val action = deleteRoles >> deleteUser >> DBIO.successful(())
        db.run(action).map(_ => Some(user))
      }
      case None => Future {
        None
      }
    }
  }

  private def findByCondition(condition: Users => Rep[Boolean]): Future[Vector[User]] = {
    val query = for {
      (user, role) <- users
        .filter(condition)
        .joinLeft(usersToRoles)
        .on(_.id === _.usersId)
    } yield (user, role.map(_.rolesCode))

    db.run(query.result).map(serializeUsers)
  }

  def findByLastName(lastName: String): Future[Seq[User]] =
    findByCondition(_.lastName === lastName).map(_.toSeq)

  def findAll(): Future[Seq[User]] =
    findByCondition(_ => true).map(_.toSeq)

  private[this] def serializeUsers(rows: Seq[(UserRow, Option[Role])]): Vector[User] =
    rows.foldLeft(Map.empty[UUID, User]) {
      case (acc, (user, role)) => acc + (
        user.id.get ->
          user.toUser(role.map {
            Set(_)
          }.getOrElse(Set())
            union
            acc.get(user.id.get).map(_.roles).getOrElse(Set())
          )
        )
    }.values.toVector

  def deleteAll(): Future[Unit] = Future {
    db.run(usersToRoles.delete >> users.delete >> DBIO.successful(()))
  }
}

object UserDaoSlickImpl {
  implicit val rolesType: BaseColumnType[Role] = MappedColumnType.base[Role, String](
    {
      case Role.Reader => "reader"
      case Role.Manager => "manager"
      case Role.Admin => "admin"
    },
    {
      case "reader" => Role.Reader
      case "manager" => Role.Manager
      case "admin" => Role.Admin
    }
  )

  case class UserRow
  (
    id: Option[UUID],
    firstName: String,
    lastName: String,
    age: Int
  ) {
    def toUser(roles: Set[Role]): User = User(id, firstName, lastName, age, roles)
  }

  object UserRow extends ((Option[UUID], String, String, Int) => UserRow) {
    def fromUser(user: User): UserRow = UserRow(user.id, user.firstName, user.lastName, user.age)
  }

  class Users(tag: Tag) extends Table[UserRow](tag, "users") {
    val id = column[UUID]("id", O.PrimaryKey, O.AutoInc)
    val firstName = column[String]("first_name")
    val lastName = column[String]("last_name")
    val age = column[Int]("age")

    val * = (id.?, firstName, lastName, age).mapTo[UserRow]
  }

  val users: TableQuery[Users] = TableQuery[Users]

  class UsersToRoles(tag: Tag) extends Table[(UUID, Role)](tag, "users_to_roles") {
    val usersId = column[UUID]("users_id")
    val rolesCode = column[Role]("roles_code")

    val * = (usersId, rolesCode)
  }

  val usersToRoles: TableQuery[UsersToRoles] = TableQuery[UsersToRoles]
}
